#include "readerfilter.h"
#include "../stlutil.h"
#include "../logging.h"
#include "../messagesfp.h"
#include "../ferapardaexception.h"
#include "../basefilter.h"
#include "../anthillutil.h"

#include <stdlib.h>
#include <sys/time.h>
#include <fcntl.h>
#include <cassert>
#include <iostream>
#include <string>
#include <sstream>
#include <cmath>
#include <zlib.h>
using namespace std;
using namespace Feraparda;
using namespace __gnu_cxx;

CREATE_ANTHILL_FILTER (ReaderFilter, filter, new AnthillUtil());

/**
 * Constructor.
 * @param config Path to the project configuration.
 */
ReaderFilter::ReaderFilter(AnthillUtil *ahUtil, void *work) {

	this->ahUtil = ahUtil;
	logger = LogFactory::GetLogger("reader");

	this->project =  (Project *) work;

	OpenOutputPorts();
	myRank = ahUtil->Rank();

	totalOfBlockerInstances = ahUtil->TotalOfReaders(blockerOutput);
	totalOfResultInstances = ahUtil->TotalOfReaders(resultOutput);
	totalOfReaderInstances = ahUtil->TotalOfReaders(recordOutput);

	/* Important: this method MUST be called after setting totalOfReaderInstances */
	OpenInputPorts();

	totalOfBothRecordsOwned = 0;
	totalOfMissedRecords = 0;

	if (false == parameters.deterministic){
		blockingMessage = new BlockingMessage(ahUtil, blockerOutput,
				totalOfBlockerInstances, GROUP_SIZE_MSG_ADD_RECORD);

		blockingMessage->ConfigureAck(1000, ahUtil->GetInputPort("ackRead"));

		resultMessage = new ResultMessage(ahUtil, resultOutput, totalOfResultInstances,
				GROUP_SIZE_MSG_RESULT);
		comparePairMessage = new ComparePairMessage(ahUtil, recordOutput,
				totalOfReaderInstances, GROUP_SIZE_MSG_RECORD);
	} else {
		deterministicBlockingMessage = new ComparePairMessage(ahUtil, blockerOutput,
				totalOfBlockerInstances, GROUP_SIZE_MSG_RECORD);
	}
	//Cache

	idsSentRecords = new Cache<unsigned int, int *> [totalOfReaderInstances];
	receivedRecords
			= new Cache<unsigned int, Record *> [totalOfReaderInstances];
	int intraReaderSize = project->getIntraReadersCacheSize();
	for (int i = 0; i < totalOfReaderInstances; i++) {
		idsSentRecords[i].setSize(intraReaderSize);
		receivedRecords[i].setSize(intraReaderSize);
		idsSentRecords[i].setDeleteOptions(false, false);
		receivedRecords[i].setDeleteOptions(true, true);
	}

	totalOfRecordsReceived = 0;
	totalOfUseCacheMetaReceived = 0;

	recordOutOpened = 1;

	overhead = 0;

	this->linkage = project->getTaskType() == LINKAGE;
	/* Open data sources */
	this->smallest = project->getDataSource("0");

	if (!parameters.indexDataSources){
		this->smallest->Load(project->getCache());
	}

	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGINFO)) {
		ostringstream o;
		o << "Reader #" << myRank << " has " << this->smallest->getRecordCount()
				<< " records in first data source";
		LOGFPINFO(logger, o.str());
	}
	#endif

	if (this->linkage){
		this->largest = project->getDataSource("1");

		if (!parameters.indexDataSources){
			this->largest->Load(project->getCache2());
		}

		#ifdef LOG_ENABLED
		if (logger->IsLevelEnabled(LOGINFO)) {
			ostringstream o;
			o << "Reader #" << myRank << " has " << this->largest->getRecordCount()
					<< " records in second data source";
			LOGFPINFO(logger, o.str());
		}
		#endif
		if (this->smallest->getRecordCount() > this->largest->getRecordCount()){
			DataSource *dsaux = this->smallest;
			this->smallest = this->largest;
			this->largest = dsaux;
		}
	}
	this->dataSource0MaxId = smallest->getRecordCount() + 1000;
	this->startRecordNumberInDs0 = 0;
}
/**
 * Default destructor
 */
ReaderFilter::~ReaderFilter() {
	if (false == parameters.deterministic){
		delete blockingMessage;
		delete resultMessage;
		delete comparePairMessage;
	} else {
		delete deterministicBlockingMessage;
	}

	delete[] idsSentRecords;
	delete[] receivedRecords;
}
/**
 *
 */
void ReaderFilter::OpenOutputPorts() {
	blockerOutput = ahUtil->GetOutputPort("blockerOutput");
	recordOutput = ahUtil->GetOutputPort("recordOutput");
	resultOutput = ahUtil->GetOutputPort("resultOutput");

}
/**
 *
 */
void ReaderFilter::OpenInputPorts() {
	
	blockerInput = ahUtil->GetInputPort("blockerInput");
	mergerInput = ahUtil->GetInputPort("mergerInput");
	if (totalOfReaderInstances > 1){
		recordInput = ahUtil->GetInputPort("recordInput");
		recordInputOpened = true;
	} else {
		recordInputOpened = false;
	}
	
	recordPairInputOpened = true;

}
/**
 * Tests if the records belongs to this instance.
 * @param record Record that will be tested
 * @return <code>true</code> if record belongs to the instance 
 */
bool ReaderFilter::IsRecordMine(int id) {
	//Remember: internally, records begin from 0.
	return (id % totalOfReaderInstances) == myRank;
}
/**
 * 
 */
Record * ReaderFilter::GetRecordUsingInternalId(int internalId){
	DataSource *ds;

	/*
	 * An internal Id is generated to identify the reader instance and data source
	 * (if it is a linkage process) that record belongs to .
	 * The data source is identified by checking the upper bound of data source 0 (dataSource0MaxId).
	 *
	 * For example, when performing a record linkage, two data sources are used. Suppose that the first one
	 * has 100 records and the second one has 121. The reader instance has ranking 3 and there are 5 instances total.
	 * - Only internal ids ending in 3 and 8 are valid (because 3 % 5 = 3, 13 % 5 = 3 and 8 % 5 = 3...
	 * - Records in data source are numbered from 0 to 99 in ds 0 and from 0 to 120 in ds 1.
	 * - dataSource0MaxId has the value 498 (498 = 99 * 5 + 3).
	 * - If a record, identified by internal id 128 arrives, it belongs to ds 0.
	 * - If a record identified by internal id 783 arrives, it belongs to ds 1.
	 */
	if (this->linkage && internalId >= dataSource0MaxId){
		int recordNumber = (internalId - myRank - dataSource0MaxId) / totalOfReaderInstances ;
		//	+ startRecordNumberInDs1;
		ds = this->largest;
		assert(recordNumber >= 0 && recordNumber < ds->getRecordCount());
		Record *r = ds->GetRecord(recordNumber);
		return r;
	} else if(this->linkage || internalId < dataSource0MaxId){
		int recordNumber = (internalId - myRank) / totalOfReaderInstances
                      + startRecordNumberInDs0;
		ds = this->smallest; 
		assert(recordNumber >= 0 && recordNumber < ds->getRecordCount());
		Record *r = ds->GetRecord(recordNumber);
		return r;
	}else {
		throw FerapardaException("Invalid internal id " + Util::ToString(internalId));
	}
}
/**
 *
 */
void ReaderFilter::ProcessDeduplication(){

	Block *block = project->getCurrentBlock();

	/* 
	 * Generate the blocking key.
	 * Each instance is responsible for a range of records.
	 */
	DataSourceBoundaries bounds = this->smallest->GetBoundaries(totalOfReaderInstances, myRank);
	
	this->startRecordNumberInDs0 = bounds.start;

	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGINFO)) {
		LOGFPINFO(logger, "Reader #" + Util::ToString(myRank) + ": records from " 
				+ Util::ToString(bounds.start) + " to " 
				+ Util::ToString(bounds.end));
	}
	#endif
	
	int counter = 0;
    int id = myRank;
	for (int i = bounds.start; i <= bounds.end; i++) {
		Record *record = this->smallest->GetRecord(i);

		SendKeysToBlocking(id, block->GenerateKeys(record, true), 0);
		/*
		 * In some cases the algorithm doesn't perform any comparison:
		 * - When the parameter -r is informed in command line
		 * - When the parameter -b is informed in command line
		 */
		if (!parameters.readAllRecordsBeforeCompare && !parameters.stopAtMerging && !parameters.startAtMerging){
			while(ahUtil->Probe(mergerInput) || ahUtil->Probe(recordInput)){
				ReceiveAndProcessMessages();
			}
		}
		counter ++;
		if ((counter % 100000) == 0) {
			cout << "Read " << counter << " records in reader " << myRank << endl;
		}
		id += totalOfReaderInstances;
	}
 	this->blockingMessage->Flush();
}
/**
 * Performs the record linkage between two databases.
 */
void ReaderFilter::ProcessLinkage(){
	
	Block *block = project->getCurrentBlock();
	
	/* 
	 * Generate the blocking key.
	 * Each instance is responsible for a range of records. The range varies from
	 * () to ().
	 */
	DataSourceBoundaries bounds0 = smallest->GetBoundaries(
			totalOfReaderInstances, myRank);
	DataSourceBoundaries bounds1 = largest->GetBoundaries(
				totalOfReaderInstances, myRank);
	
	this->startRecordNumberInDs0 = bounds0.start;
	this->startRecordNumberInDs1 = bounds1.start;
	
	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGINFO)) {
		LOGFPINFO(logger, "Reader #" + Util::ToString(myRank) + ": records for datasource 0 from " 
				+ Util::ToString(bounds0.start) + " to " 
				+ Util::ToString(bounds0.end));
		LOGFPINFO(logger, "Reader #" + Util::ToString(myRank) + ": records for datasource 1 from " 
						+ Util::ToString(bounds1.start) + " to " 
						+ Util::ToString(bounds1.end));
	}
	#endif	
	int counter = 0;
	int id = myRank;
	smallest->setPosition(0); //Em teste
	for (int i = bounds0.start; i <= bounds0.end; i++) {
		//Record *record = smallest->GetRecord(i);
		Record *record = smallest->GetNextRecord();
		//cout << "REGISTRO: " << i << " " << record->getField("id") << endl;
		SendKeysToBlocking(id, block->GenerateKeys(record, true), 0);
		id += totalOfReaderInstances;
		counter ++;
		#ifdef LOG_ENABLED
		if ((counter % 100000) == 0 && logger->IsLevelEnabled(LOGSTAT)) {
			LOGFPSTAT(logger, "For datasource 0, read " + Util::ToString(counter) + 
					" records in reader " + Util::ToString(myRank));
		}
		#endif		
	}
	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGSTAT)) {
		LOGFPSTAT(logger, "For datasource 0, read and processed " + Util::ToString(counter) + 
				" records up to now in reader " + Util::ToString(myRank));
	}
	#endif		

	this->blockingMessage->Flush();

    id = this->dataSource0MaxId;
	counter = 0;
	//Note: id = myRank is no necessary, because datasource 1 will be 
	// "appended" to datasource 0
	cout << "Using ids + " << this->dataSource0MaxId << endl;
	largest->setPosition(0);
	for (int i = bounds1.start; i <= bounds1.end; i++) {
		//Record *record = largest->GetRecord(i);
		Record *record = largest->GetNextRecord();
		SendKeysToBlocking(id, block->GenerateKeys(record, true), 1);
		if (!parameters.readAllRecordsBeforeCompare){
			ReceiveAndProcessMessages();
		}
		id += totalOfReaderInstances;
		counter ++;
		#ifdef LOG_ENABLED
		if (((counter) % 100000) == 0 && logger->IsLevelEnabled(LOGSTAT)) {
			LOGFPSTAT(logger, "For datasource 1, read " + Util::ToString(counter) + 
					" records up to now in reader " + Util::ToString(myRank));
		}
		#endif		
	}
	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGSTAT)) {
		LOGFPSTAT(logger, "For datasource 1, read and processed " + Util::ToString(counter) + 
				" records in reader " + Util::ToString(myRank));
	}
	#endif		

	
 	this->blockingMessage->Flush();
}
/**
 * Sends entire record to blocking filter. It will perform deterministic deduplication/linkage,
 * i.e., compare a set of fields and if two or more records match in these fields, the records
 * reference the same entity.
 */
void ReaderFilter::ProcessDeterministic() {
	DataSourceBoundaries bounds0 = smallest->GetBoundaries(
			totalOfReaderInstances, myRank);
	int counter = 0;
	int id = myRank;
	this->startRecordNumberInDs0 = bounds0.start;
	for (int i = bounds0.start; i <= bounds0.end; i++) {
		Record *record = smallest->GetRecord(i);
		SendRecordToBlocking(id, record, false);
		id += totalOfReaderInstances;
		counter++;
#ifdef LOG_ENABLED
		if (counter % 100000 == 0 && logger->IsLevelEnabled(LOGSTAT)) {
			LOGFPSTAT(logger, "For datasource 0, read " + Util::ToString(
					counter) + " records up to now in reader " + Util::ToString(myRank));
		}
#endif
	}
#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGSTAT)) {
		LOGFPSTAT(logger, "For datasource 0, read and processed " + Util::ToString(
				counter) + " records in reader " + Util::ToString(myRank));
	}
#endif

	this->deterministicBlockingMessage->Flush();


	if (linkage) {
		DataSourceBoundaries bounds1 = largest->GetBoundaries(
				totalOfReaderInstances, myRank);

		this->startRecordNumberInDs1 = bounds1.start;

		id = this->dataSource0MaxId;
		counter = 0;
		for (int i = bounds1.start; i <= bounds1.end; i++) {
			Record *record = largest->GetRecord(i);
			SendRecordToBlocking(id, record, true);
			id += totalOfReaderInstances;
			counter++;
#ifdef LOG_ENABLED
			if (counter % 100000 == 0 && logger->IsLevelEnabled(LOGSTAT)) {
				LOGFPSTAT(logger, "For datasource 1, read " + Util::ToString(
						counter) + " records up to now in reader " + Util::ToString(
						myRank));
			}
#endif
		}
#ifdef LOG_ENABLED
		if (logger->IsLevelEnabled(LOGSTAT)) {
			LOGFPSTAT(logger, "For datasource 1, read and processed " + Util::ToString(
					counter) + " records in reader " + Util::ToString(
					myRank) + " (Deterministic)");
		}
#endif

		this->deterministicBlockingMessage->Flush();
	}
}
/**
 * Processes requests
 */
void ReaderFilter::Process() {
	/* Indexes all data sources and DO NOT perform anything else !*/
	if (parameters.indexDataSources){
		cout << "----------------------------------------------" << endl
			<< "Indexing data source @Reader " << this->myRank << endl
			<< "----------------------------------------------" << endl;
		this->smallest->Index();
		if (this->linkage){
			this->largest->Index();
		}
		return;
	} else if (parameters.deterministic){
		ProcessDeterministic();
		return;
	}
	try {
		if (!parameters.startAtMerging){
			if (this->linkage) {
				ProcessLinkage();
			} else {
				ProcessDeduplication();
			}
		} else {
			ReadCandidatePairs();
		}
		/*
		 * There aren't any record to process anymore, so it's possible to close the
		 * output handler. Doing this will prevent the program to stay in an
		 * infinity loop.
		 */
		blockingMessage->Flush();
		ahUtil->Close( blockerOutput);

		if (!parameters.readAllRecordsBeforeCompare && !parameters.stopAtMerging && !parameters.startAtMerging){
			while (recordPairInputOpened || recordInputOpened) {
				ReceiveAndProcessMessages();
			}
		}
		resultMessage->Flush();

#ifdef LOG_ENABLED
		if (logger->IsLevelEnabled(Feraparda::LOGSTAT)) {
			LOGFPSTAT(logger, "----------- STATISTICS @" +
					Util::ToString(myRank) + "-----------------");
			LOGFPSTAT(logger, "Total of owned pairs: " + Util::ToString(totalOfBothRecordsOwned));
			LOGFPSTAT(logger, "Total of RecordMessage sent: " +
					Util::ToString(this->comparePairMessage->getTotalOfRecordMessages()));
			LOGFPSTAT(logger, "Total of RecordMessage received: " +
					Util::ToString(this->totalOfRecordsReceived));
			LOGFPSTAT(logger, "Total of UseCacheMessage sent: " +
					Util::ToString(this->comparePairMessage->getTotalOfUseCacheMessages()));
			LOGFPSTAT(logger, "Total of UseCacheMessage received: " +
					Util::ToString(totalOfUseCacheMetaReceived));
			LOGFPSTAT(logger, "Total of ResultMessage sent: " +
					Util::ToString(this->resultMessage->getTotalOfSentMessages()));
			LOGFPSTAT(logger, "Total of BlockingMessage sent: " +
					Util::ToString(this->blockingMessage->getTotalOfSentMessages()));
			LOGFPSTAT(logger, "Total of Missing Records: " +
					Util::ToString(this->totalOfMissedRecords));
			LOGFPSTAT(logger, "--------------------------------------------------");
		}
#endif
		cout << "Overhead compare " << overhead * 0.000001 << " s. " << endl;
	} catch (FerapardaException &ex) {
		cerr << "--------------------------------------------------------"
				<< endl;
		cerr << "An exception had been threw in rank #" << filter->getRank()
				<< "  " << ex.Report() << endl;
		cerr << "--------------------------------------------------------"
				<< endl;
	} catch (FerapardaException *ex) {
		cerr << "--------------------------------------------------------"
				<< endl;
		cerr << "An exception had been threw in rank #" << filter->getRank()
				<< "  " << ex->Report() << endl;
		cerr << "--------------------------------------------------------"
				<< endl;
	} catch (exception &ex) {
		cout << "An severe exception had been thew: " << ex.what() << endl;
	}
}
/**
 * Processes incoming messages
 */
void ReaderFilter::ReceiveAndProcessMessages() {

	try {
		if (!parameters.startAtMerging){
			ReceiveAndProcessCompareMessage();
			if (totalOfReaderInstances > 1){
				ReceiveAndProcessCompareMessageFromOtherInstances();
			}
		}
	} catch(FerapardaException &ex) {
		cout << "---------------------------------------------------------" << endl;
		cout << "An exception had been threw in rank# " << myRank << ": " << ex.Report() << endl;
		cout << "---------------------------------------------------------" << endl;
	}
}
void ReaderFilter::Compare(result_msg_t *resultMsg, Record *recordOne,
		Record *recordTwo, unsigned int id1, unsigned int id2,
		unsigned int blockId) {

	struct timeval start;
	struct timeval end;
	
	//cout << "@@@@@@@ " << id1 << " " << id2 << endl << flush;

	assert(!linkage || (this->smallest->HasRecord(id1) && this->largest->HasRecord(id2 - this->dataSource0MaxId)));
	gettimeofday(&start, NULL);

	resultMsg->id1 = id1;
	resultMsg->id2 = id2;
	resultMsg->blockId = blockId;

	strncpy(resultMsg->key1, recordOne->getField("id").c_str(), MAX_KEY_SIZE -1);
	strncpy(resultMsg->key2, recordTwo->getField("id").c_str(), MAX_KEY_SIZE -1);

	//
	resultMsg->result = project->Compare(recordOne, recordTwo);

	if (parameters.saveComparisonScores) {
		sprintf(resultMsg->resultVector, "%s", project->getResultVector().c_str());
	}

	resultMsg->weightResult = resultMsg->result;
	//cout << "### : " << resultMsg->key1 << ", " << resultMsg->key2 << " " << resultMsg->weightResult << endl;
	gettimeofday(&end, NULL);
	overhead += (end.tv_sec -start.tv_sec) * 1000000 + 
					(end.tv_usec -start.tv_usec);
}
void ReaderFilter::CompareMessage(record_pair_msg_t msg){
	result_msg_t outMsg;
	//cout << "BlockCache " << msg.msgs[i].blockId << endl;
	/*
	 * Test if this instance has the record identified by ids2.
	 * (it should, otherwise, label stream is not working !).
	 * Note that the record can be read from the cache (there is a cache
	 * instance in Feraparda::DataSource).
	 * The record number starts from 1 and the ids in the message
	 * carries information about the rank and number of instance of readers.
	 * So, it's necessary to extract the id.
	 */
	//Note: recNum1 uses ids2!!!
	//assert(msg.msgs[i].id2 >= msg.msgs[i].id1);
	//cout << "@@@@@@@@@@ " << msg.msgs[i].id2 << " " <<  msg.msgs[i].id1 << " " << myRank << endl;
	Record *recordOne = GetRecordUsingInternalId(msg.id2);

	assert(recordOne != NULL);
	/*If this instance has both records, no more communication is needed */
	if (IsRecordMine(msg.id1)) {
		//cout << ">>>>>>>> Comparando " << msg.id1 << " " << msg.id2 << endl;
		Record *recordTwo = GetRecordUsingInternalId(msg.id1);
		this->Compare(&outMsg, recordOne, recordTwo,
				msg.id1, msg.id2, msg.blockId);

		this->totalOfBothRecordsOwned ++;
		this->resultMessage->Send(outMsg);
	} else {
		if (idsSentRecords[msg.id1 % totalOfReaderInstances].HasKey(msg.id2)) {
#ifdef LOG_ENABLED
			if (logger->IsLevelEnabled(LOGDEBUG)) {
				ostringstream o;
				o << "Sending redo (" << msg.id1 << ", "
						<< msg.id2 << ") to instance "
						<< (msg.id1
								% totalOfReaderInstances);
				LOGFPDEBUG(logger, o.str());
			}
#endif
			/** Note: here the order is the id of owned record and id of the other one! */
			SendUseCacheToReader(msg.id2, msg.id1,0);
		} else {

#ifdef LOG_ENABLED
			if (logger->IsLevelEnabled(LOGDEBUG)) {
				ostringstream o;
				o << "Reader #" << myRank << " is sending record "
						<< msg.id2 << " to instance "
						<< (msg.id1
								% totalOfReaderInstances);
				LOGFPDEBUG(logger, o.str());
			}
#endif
			//cout << "Reader #" << myRank << " is sending record " << msg.id2 << " to instance "
			//		<< (msg.id1 % totalOfReaderInstances) << " " <<  msg.id1 << " "  << recordOne->getField("id") << endl;
			/*
			 * Note: here the order is the id of owned record and id of the other one!
			 */
			SendRecordToReader(recordOne, msg.id2, msg.id1, 0);
		}
	}
}
/**
 * Process compare messages
 */
void ReaderFilter::ReceiveAndProcessCompareMessage() {
	record_pair_grouped_msg_t msg;
	
	AnthillInputPort *inPort;
	/*
	 * Read the data from the shortcut (skips merger filter). Set skipMerger phase true.
	 * Note that you need to set this variable in blockingfilter.cc also.
	 */
	bool skipMerger = false;
	if (skipMerger && this->totalOfBlockerInstances == 1) {
		inPort = &blockerInput;
	} else {
		inPort = &mergerInput;
	}
	if (recordPairInputOpened && ahUtil->Probe(*inPort)) {
		if (ahUtil->Read(*inPort, &msg, sizeof(record_pair_grouped_msg_t))
				!= EOW) {
			for (int i = 0; i < msg.total; i++) {
				CompareMessage(msg.msgs[i]);
			}
		} else {
#ifdef LOG_ENABLED
			if (logger->IsLevelEnabled(LOGDEBUG)) {
				LOGFPDEBUG(logger, "Merger's work finished, closing output ports");
			}
#endif			
			recordPairInputOpened = false;
			/* Flush the remaining messages*/
			this->comparePairMessage->Flush();
			recordOutOpened = 0;
			ahUtil->Close(recordOutput);
		}
	}
}
void ReaderFilter::ReadCandidatePairs(){
	char buf[4096];
	int err;

	int fd = open64(project->getOutput()->getCandidates().c_str(), O_RDONLY, 0666);
	if (fd < 0){
		throw FerapardaException(string("Error opening file (") +
				project->getOutput()->getCandidates() +
				string(") for outputing candidate pairs: ") + string(strerror(errno)));
	}
	gzFile inFile = gzdopen(fd, "r");

	if (inFile == NULL){
		throw FerapardaException("File not found when reading candidate pairs at Reader Filter");
	}
	record_pair_msg_t msg;
	totalOfBothRecordsOwned = 0;
	for (;;) {
		if (!gzgets(inFile, buf, sizeof(buf))){
			gzerror(inFile, &err);
			if (err > 1){
				throw FerapardaException(gzerror(inFile, &err));
			} else {
				break;
			}
		} else {
			int id1, id2, block;
			if (buf[0] != '\n' && buf[0] != '\0'){ //Ignores empty lines
				//cout << ">>>>> Read: " << (int) buf[0] << "|\n";
				//FIXME: Como fazer isto em C++? Utilizar istringstream?
				sscanf(buf, "%d %d %d", &id1, &id2, &block);
				msg.id1 = id1;
				msg.id2 = id2;
				msg.blockId = block;
				this->CompareMessage(msg);

				if (((totalOfBothRecordsOwned + 1)% 1000000) == 0){
					cout << "Compared " << totalOfBothRecordsOwned << " records at reader filter " << myRank << endl;
				}
			}
		}
	}
	gzclose(inFile);
}
/**
 *
 *
 */
void ReaderFilter::ReceiveAndProcessCompareMessageFromOtherInstances() {
  if(recordInputOpened && ahUtil->Probe(recordInput)) {
	compare_pair_grouped_msg_t msg;
	if (ahUtil->Read(recordInput, &msg, sizeof(compare_pair_grouped_msg_t))
		!= EOW) {
	  int source = msg.source;
	  int offset = 0;
	  while(offset < msg.total){
		char type = msg.data[offset];
		offset += sizeof(char);

		short size;
		memcpy(&size, &(msg.data[offset]), sizeof(short));
		offset += sizeof(short);
		assert(size >0);

		if (type == MESSAGE_USE_CACHE){
		  totalOfUseCacheMetaReceived ++;
		  use_cache_msg_t useCacheMsg;
		  memcpy(&useCacheMsg, &(msg.data[offset]), size);


		  if (!receivedRecords[source].HasKey(useCacheMsg.id1)) {
			cout << "@@@ Problema cache nao tem chave " << useCacheMsg.id1  << endl; 
				//<< " " << useCacheMsg.id2<< receivedRecords[source].DumpKeys() << endl;
			totalOfMissedRecords ++;
		  } else {


			assert(!IsRecordMine(useCacheMsg.id1));
			assert(IsRecordMine(useCacheMsg.id2));
			Record *mine = GetRecordUsingInternalId(useCacheMsg.id2);
			Record *cached =
			  receivedRecords[source].Get(useCacheMsg.id1);
			/* 
			if(myRank == 1) {
			 	cout << "$# " << receivedRecords[source].DumpKeys() << endl;
			}*/

			result_msg_t outMsg;
			this->Compare(&outMsg, mine, cached, useCacheMsg.id1,
				useCacheMsg.id2, useCacheMsg.blockId);
			this->resultMessage->Send(outMsg);
		  }
		} else if (type == MESSAGE_RECORD){
		  totalOfRecordsReceived ++;	
		  DataSource *ds;
		  record_msg_t recordMsg;
		  memcpy(&recordMsg, &(msg.data[offset]), size);

		  //FIXME !!!
		  if (recordMsg.id1 >= (unsigned) dataSource0MaxId) {
			ds = this->largest;
		  } else {
			ds = this->smallest;
		  }
		  //FIXME: Verificar se isto está funcionando depois da alteração em datasource
		  Record *recordTwo = new Record(recordMsg.id1, 
			  recordMsg.record, ds->getFieldsInfo(), ds->getRecordLength());

		  assert(recordTwo != NULL);
		  this->receivedRecords[recordMsg.id1 % totalOfReaderInstances].Put(
			  recordMsg.id1, recordTwo);
			  /*
		  if (myRank == 1){
			cout << "$$$ " << msg.seq<< " - " << recordMsg.id1 << " " << recordMsg.id2  <<"|" << 
				receivedRecords[recordMsg.id1 % totalOfReaderInstances].DumpKeys() << endl;
		  }*/
		  /*
		   * The record id always carries information about the filter instance.
		   * Use this formula to extract the record number. 
		   */
		  Record *recordOne =  GetRecordUsingInternalId(recordMsg.id2);
		  result_msg_t resultMsg;
		  this->Compare(&resultMsg, recordOne, recordTwo,
			  recordMsg.id1, recordMsg.id2, recordMsg.blockId);
		  this->resultMessage->Send(resultMsg);

		} else {
		  //throw new FerapardaException("Invalid message type received " + (int) type);
		  cout << "Invalid message type " << (int) type << " " << offset << endl;
		}
		offset += size;
	  }
	} else {
	  //recordOutOpened = 0;
	  recordInputOpened = false;
	  comparePairMessage->Flush();
	}
  }
}
/**
 * Sends a record to another instance of reader filter.
 * @param record Record that will be sent
 * @param id1 Record 1 identifier (owned)
 * @param id2 Record 2 identifier (missing)
 */
void ReaderFilter::SendRecordToReader(Record *record, unsigned int id1,
		unsigned int id2, unsigned int blockId) {

	assert(recordOutOpened);	
	record_msg_t msg;
	strcpy(msg.record, record->Pack().c_str());
	msg.id1 = id1;
	msg.id2 = id2;
	msg.blockId = blockId;
	this->comparePairMessage->Send(msg); 
	this->idsSentRecords[id2 % totalOfReaderInstances].Put(id1, new int(id2 % totalOfReaderInstances));
	/*
	if (myRank == 0){
		//cout << "Enviando registro " << record->getId() << " " << id1 << endl;
		cout << id1 << " " << id2 << " | " << this->idsSentRecords[id2 % totalOfReaderInstances].DumpKeys() << endl;
	}
	*/
}
/**
 * @param id1 Id of owned record
 * @param id2 Id of the other record
 */
void ReaderFilter::SendUseCacheToReader(unsigned int id1, unsigned int id2,
		unsigned int blockId) {
	use_cache_msg_t msg;
	msg.id1 = id1;
	msg.id2 = id2;
	msg.blockId = blockId;
	
	this->idsSentRecords[id2 % totalOfReaderInstances].Put(id1, new int(id2 % totalOfReaderInstances));
	/*
	if (myRank == 0){
		cout << "> " << this->idsSentRecords[id2 % totalOfReaderInstances].DumpKeys() << endl;
	}
	*/
	assert(recordOutOpened);
	this->comparePairMessage->Send(msg);
}
/**
 * Sends a record to blocking when performing a deterministic deduplication/linkage operation.
 */
void ReaderFilter::SendRecordToBlocking(int id, Record *record, bool dataSource1){
	record_msg_t msg;
	hash<const char*> H;

	strcpy(msg.record, record->Pack().c_str());
	msg.id1 = atoi(record->getField("id").c_str());
	//Sends all records with same hash to only one instance of blocking
	msg.id2 = H(msg.record);
	msg.blockId = 0;
	if (dataSource1){
		BLOCKING_FLAG_DATASOURCE_1(msg.blockId);
	}

	this->deterministicBlockingMessage->Send(msg);
}
/**
 * Send block keys. Notice that <code>blockIdMask</code> controls some details
 * about the blocking process using a bit mask: 
 * 10000000 (most significant bit set): 0 - deduplication 1 - linkage
 * 01000000 : 0 - key generate by the smallest datasource 1 - generated by largest one
 * @param id Record id
 * @param keys Keys that will be sent
 * @param blockIdMask Extra information about the block. 
 */
void ReaderFilter::SendKeysToBlocking(int id, vector<string> keys, 
		short dataSource) {
	block_msg_t msg;
	msg.recnum = id;

	assert(dataSource == 0 || dataSource == 1);
	for (unsigned int i = 0; i < keys.size(); i++) {
		if (keys[i].size() > MIN_BLOCK_KEY_SIZE){
	#ifdef LOG_ENABLED
			if (logger->IsLevelEnabled(LOGDEBUG)) {
				ostringstream o;
				o << "Rec. " << id << " generated key: " << keys[i];
				LOGFPDEBUG(logger, o.str());
			}
	#endif
			assert(id >= 0);
			strcpy(msg.key, keys[i].c_str());

			/*
			 * Encodes the blocking flags. See documentation.
			 */
			msg.flags = 0;
			if (this->linkage){
				BLOCKING_FLAG_LINKAGE(msg.flags);
			}
			if (dataSource == 1){
				BLOCKING_FLAG_DATASOURCE_1(msg.flags);
			}
			BLOCKING_FLAG_BLOCKING_ID(msg.flags, i);
			blockingMessage->Send(msg);
		}
	}
}
void BlockingMessage::ConfigureNeedAck(block_grouped_msg_t group, bool needsAck){
//	group.requiresAck = needsAck ? '\1': '\0';
}
/**
 * 
 */
bool BlockingMessage::Add(block_msg_t msg) {
	/*
	 * Target defines the instance that will receive the message
	 * Here we use a simple strategy (MOD of the first character). 
	 * Maybe there is a better one.
	 */
	hash<const char*> H;
	int target = H(msg.key) % this->totalOfTargets;
	this->lastGroup = target;
	this->groups[lastGroup].msgs[counter[lastGroup]] = msg;
	this->counter[lastGroup] ++;
	this->groups[lastGroup].total ++;
	return (counter[lastGroup] >= this->max);
}
void ComparePairMessage::Flush(){
	GroupedMessage<compare_pair_msg_t, compare_pair_grouped_msg_t>::Flush(); 
}
void ComparePairMessage::Flush(int target){
  if (counter[target] > 0){
	totalOfSentMessages ++;
	compare_pair_grouped_msg_t msg;
	memcpy(&msg, &(groups[target]), sizeof(msg));
	msg.seq = totalOfSentMessages;
	ahUtil->Send(getOut(), &msg, sizeof(msg));
	//GroupedMessage<compare_pair_msg_t, compare_pair_grouped_msg_t>::Flush(target); 
	counter[target] = 0;
	groups[target].total = 0;
  }
}
/**
 * 
 */
bool ComparePairMessage::Add(record_msg_t msg) {
	int target = msg.id2 % totalOfTargets;
	int size = sizeof(msg);
	
	this->lastGroup = target;

	//Current block does not fit the new message? Flush it!
	if (this->groups[target].total + size >= COMPARE_PAIR_GROUPED_DATA_SIZE){
		this->Flush(target);
		this->groups[target].total = 0;
	}
	int offset = this->groups[lastGroup].total;
	this->groups[target].mod = target;
	this->groups[lastGroup].data[offset] = MESSAGE_RECORD;
	offset ++;
	memcpy(&(this->groups[lastGroup].data[offset]), &size, sizeof(short));
	offset +=sizeof(short);
	memcpy(&(this->groups[lastGroup].data[offset]), &msg, size);

	offset += size;

	this->groups[lastGroup].total = offset;
	counter[lastGroup] = groups[lastGroup].total;
	

	return false;
}

/**
 * 
 */
bool ComparePairMessage::Add(use_cache_msg_t msg) {
	int target = msg.id2 % totalOfTargets;
	int size = sizeof(msg);

	this->lastGroup = target;

	//Current block does not fit the new message? Flush it!
	if (this->groups[target].total + size >= COMPARE_PAIR_GROUPED_DATA_SIZE){
		this->Flush(target);
		this->groups[target].total = 0;
	}
	int offset = this->groups[lastGroup].total;

	this->groups[target].mod = target;
	this->groups[lastGroup].data[offset] = MESSAGE_USE_CACHE;
	offset ++;
	memcpy(&(this->groups[lastGroup].data[offset]), &size, sizeof(short));
	offset +=sizeof(short);
	memcpy(&(this->groups[lastGroup].data[offset]), &msg, size);
	
	offset += size;
	this->groups[lastGroup].total = offset;

	counter[lastGroup] = groups[lastGroup].total;
	
	return false;
}



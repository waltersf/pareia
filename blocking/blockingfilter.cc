/*
 * blockingfilter.c
 * (c) e-Speed/DCC/UFMG
 * (c) Walter dos Santos Filho - waltersf AT gmail.com
 */
#include "blockingfilter.h"
#include "groupedmessage.h"
#include "logging.h"
#include "stlutil.h"
#include "anthillutil.h"
#include "project.h"
#include "../reader/ResultMessage.h"
#include "../sqlite/sdsqlite.h"
#include <sstream>
#include <cassert>
#include <map>
using namespace std;
using namespace Feraparda;

CREATE_ANTHILL_FILTER (BlockingFilter, filter, new AnthillUtil())

/*
 * Blocking Filter implementation. Constructor.
 */
BlockingFilter::BlockingFilter(AnthillUtil *util, void *data) {

	this->SetAnthillUtil(util);
	this->myRank = ahUtil->Rank();
	this->totalOfBlockingFilterInstances = ahUtil->TotalOfFilterInstances();
	this->logger = LogFactory::GetLogger("blocking");

	OpenPorts();

	this->totalInstancesOfMerger = ahUtil->TotalOfReaders(out);
	this->totalOfBlockingMsgReceived = 0;
	this->totalOfDuplicatedPairs = 0;

	this->blocksGenerated = new int[MAX_BLOCK_TYPES];
	this->pairsGenerated = new int[MAX_BLOCK_TYPES];
	for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
		this->blocksGenerated[i] = 0;
		this->pairsGenerated[i] = 0;
	}
}
void BlockingFilter::OpenPorts() {
	in = ahUtil->GetInputPort("readerInput");

	if (parameters.deterministic == false){
		out = ahUtil->GetOutputPort("mergerOutput");
	} else {
		resultOut = ahUtil->GetOutputPort("resultOutput");
	}
}
/**
 *
 */
void BlockingFilter::Process() {
	if (parameters.deterministic == false){
		ProcessBlocking();
	} else {
		ProcessDeterministicBlocking();
	}
}
/**
 *
 */
void BlockingFilter::ProcessDeterministicBlocking(){

	int counter = 0;
	compare_pair_grouped_msg_t msg;
	AnthillOutputPort resultOutput = ahUtil->GetOutputPort("resultOutput");
	int totalOfResultInstances = ahUtil->TotalOfReaders(resultOutput);

	ResultMessage *resultMessage = new ResultMessage(ahUtil, resultOutput, totalOfResultInstances,
					GROUP_SIZE_MSG_RESULT);

	/* Create a SQLite DB for storing the keys. In large databases, the key set doesn't fit in RAM */
	#ifdef LOG_ENABLED
	if (logger->IsLevelEnabled(LOGDEBUG)) {
		LOGFPDEBUG(logger, "Using tmp dir = " + project->getTmpDir() +
				" (check if it exists before running!).");
	}
	#endif

	sd::sqlite database(project->getTmpDir() + "/deterministic-blocking-pareia.db");
	sd::sql queryKeysStmt (database);
	sd::sql insertKeysStmt (database);
	/* Remove previous table if it exists */
	try{
		/* Recreates the table. Temporary tables are faster because there is no journaling. */
		database << "drop table if exists deterministic ";
		database << "create table deterministic (blockingKey varchar(10000), idRecord integer NULL, recordKey varchar(10000) NULL)";
		database << "create index inx_key on deterministic(blockingKey)";

		//insertKeysStmt << "insert into deterministic(blockingKey, idRecord) values(?, ?)";
		result_msg_t outMsg;
		queryKeysStmt << "select recordKey, idRecord from deterministic d where d.blockingKey = ? ";
		insertKeysStmt << "insert into deterministic(blockingKey, idRecord, recordKey) values(?, ?, ?)";

		database << "PRAGMA cache_size=100000";
		database << "PRAGMA synchronous=OFF";
		database << "PRAGMA journal_mode=OFF";
		database << "PRAGMA temp_store=MEMORY";
		database << "begin transaction";
		DataSource *smallest = project->getDataSource("0");
		DataSource *largest;
		if (project->getTaskType() == LINKAGE) {
			largest = project->getDataSource("1");
			if (smallest->getRecordCount() > largest->getRecordCount()){
				DataSource *dsaux = smallest;
				smallest = largest;
				largest = dsaux;
			}
		}
		while (ahUtil->Read(in, &msg, sizeof(compare_pair_grouped_msg_t))
				!= EOW) {
			int offset = 0;
			short size;
			Block *deterministic = project->getCurrentDeterministic();

			while (offset < msg.total) {
				char type = msg.data[offset];
				offset += sizeof(char);

				memcpy(&size, &(msg.data[offset]), sizeof(short));
				offset += sizeof(short);
				if (type == MESSAGE_RECORD) {
					record_msg_t recordMsg;
					memcpy(&recordMsg, &(msg.data[offset]), size);

					Record *record;
					if (IS_BLOCKING_FLAG_DATA_SOURCE_1(recordMsg.blockId)){
						record = new Record(recordMsg.id1,
							recordMsg.record, largest->getFieldsInfo());
						//cout << ">>>>> DS1 >>>>>>>>>" << recordMsg.id1 << endl;
					} else {
						record = new Record(recordMsg.id1,
							recordMsg.record, smallest->getFieldsInfo());
						//cout << ">>>>> DS0 >>>>>>>>>" << recordMsg.id1 << endl;
					}
					vector<string> keys = deterministic->GenerateKeys(record);
					for (unsigned int i = 0; i < keys.size(); i++) {
						/* Statement used to retrieve rows */
						queryKeysStmt.reset(); //Otherwise, the first bound parameter will be used;
						queryKeysStmt << keys[i];

						if ((IS_BLOCKING_FLAG_DATA_SOURCE_1(recordMsg.blockId) || project->getTaskType() == DEDUPLICATION)
								&& (queryKeysStmt.step())){//FOUND!
							int id;
							string recordKey;
							queryKeysStmt >> id;
							queryKeysStmt >> recordKey;

							//cout << "FOUND: " << keys[i] << " = " << bkey << " id: " << id << " = " << recordMsg.id1 << endl;

							//cout << "Registros com mesma chave (" << recordKey << ", "
							//	<< record->getField("id") << ") Hash: " << recordMsg.id2 << endl;

							outMsg.id1 = id;
							outMsg.id2 = recordMsg.id1;

							strcpy(outMsg.key1, recordKey.c_str());
							strcpy(outMsg.key2, record->getField("id").c_str());

							resultMessage->Send(outMsg);
							queryKeysStmt.reset();
						} else if(!IS_BLOCKING_FLAG_DATA_SOURCE_1(recordMsg.blockId) && keys[i] != "") {
							//inserts the new blocking key
							try{
								insertKeysStmt << keys[i] << recordMsg.id1 << record->getField("id");
								insertKeysStmt.step();
							} catch(std::exception &err){
								cout << "Exception: " << err.what() << " " << keys[i] << " " << recordMsg.id1 << " " << record->getField("id") << endl << flush;
								throw err;
							}
							//cout << "@@" << keys[i] << "|" << recordMsg.id1 << "|" << record->getField("id") << "@@" << endl;
						}
						//cout << "Key " << keys[i] << " " << keys[i].size() << endl << flush;
					}
					delete record;
				} else {
					throw new FerapardaException("Invalid message type received " + (int) type);
				}
				offset += size;
				counter ++;
				if (counter == 100000){
					counter = 0;
					database << "commit transaction";
					database << "begin transaction";
				}
			}
		}
		//Clean the database
		insertKeysStmt.finalize();
		queryKeysStmt.finalize();

		//Commiting any left insert
		database << "commit transaction";

	}catch(sd::db_error e){
			throw FerapardaException(string("Database error in blocking filter: ") + e.what());
	}
	resultMessage->Flush();
	delete resultMessage;
}
/**
 *
 */
void BlockingFilter::ProcessBlocking() {

	block_grouped_msg_t msg;
	vector<BlocksMap> blocks(MAX_BLOCK_TYPES); //stores generated blocks

	MergerMessage mergerMessage(ahUtil, out, this->totalInstancesOfMerger,
			GROUP_SIZE_MSG_PAIR);

	AnthillOutputPort ackWrite = ahUtil->GetOutputPort("ackWrite");

	long counter = 0;
	bool linkage = false;
	while (ahUtil->Read(in, &msg, sizeof(block_grouped_msg_t)) != EOW) {
		/* Sanity check: Is labeled stream working? */
		assert(myRank == msg.mod);

		this->totalOfBlockingMsgReceived += msg.total;
		linkage = IS_BLOCKING_FLAG_LINKAGE(msg.msgs[0].flags);

		counter += msg.total;
		for (int i = 0; i < msg.total; i++) {
			bool datasource0 = ! IS_BLOCKING_FLAG_DATA_SOURCE_1(msg.msgs[i].flags);

			//assert(msg.msgs[i].key[0] % totalOfBlockingFilterInstances == myRank);
			int blockId = GET_BLOCKING_ID(msg.msgs[i].flags);
			if (blockId >= MAX_BLOCK_TYPES) {
				throw new FerapardaException(
						"The maximum number of conjunctions was exceeded: "
								+ Util::ToString(blockId));
			}
			//pair<int, char *> hashKey(blockId, strdup(msg.msgs[i].key));
			char *key = strdup(msg.msgs[i].key);
#ifdef USE_FASTTRIE
			int keySize = strlen(key);
			ListOfRecordIds *listOfIds = blocks[blockId].find(key, keySize);
#else
			ListOfRecordIds *listOfIds = blocks[blockId][key];
#endif
			/*
			 * In a deduplication task, datasource0 is the only datasource used.
			 * In a linkage task, it should be the smallest one for performance.
			 */
			if (datasource0) {
				/*
				 * There is at least a record that generated the same blocking key
				 * for the same conjunction? If yes, there is a list storing the
				 * record id.
				 */
				if (listOfIds == NULL) {
					/* Creates a new list of ids that generated the same key */
					listOfIds = new ListOfRecordIds();
#ifdef USE_FASTTRIE
					blocks[blockId].add(key, keySize, listOfIds);
					free(key); //created by strdup but not needed anymore
#else
					blocks[blockId][key] = listOfIds;
#endif
					if (logger->IsLevelEnabled(LOGDEBUG)) {
						ostringstream o;
						o << "New block[" << (int) blockId << "] "
								<< blocksGenerated[blockId]
								<< " created for key |" << key
								<< "| ";
						LOGFPDEBUG(logger, o.str());
					}
					blocksGenerated[blockId]++;
				} else {
					if (false == linkage && false == parameters.readAllRecordsBeforeCompare) {
							GenerateCombinations(listOfIds->begin(), listOfIds->end(), msg.msgs[i].recnum, blockId,
								&mergerMessage);
					}
					free(key); //created by strdup but not needed anymore
				}
				/* Adds the id of record */
				listOfIds->push_back(msg.msgs[i].recnum);
			} else {
				/*
				 * If the block was generated by datasource 1, the record will
				 * be compared against all records that generated the same key
				 * in datasource 0. If there is no record in datasource 0 that
				 * generate the same key, the record from datasource 1 is
				 * ignored.
				 */
				//BUG: Nao tem como esperar todos os ids antes de comparar em linkage if (listOfIds != NULL && false == parameters.readAllRecordsBeforeCompare) {
				if (listOfIds != NULL) {
				//	cout << "Generating combinations for " << msg.msgs[i].recnum << endl;
					GenerateCombinations(listOfIds->begin(), listOfIds->end(), msg.msgs[i].recnum, blockId,
							&mergerMessage);
				}
			}
		}
		if (msg.requiresAck) {
			ahUtil->Send(ackWrite, &msg.requiresAck, sizeof(char));
		}
		if ((counter % 100000) == 0){
			cout << counter << " blocking keys received at blocking " << myRank << endl;
		}
	}
	/* Only after read all records, the block filter instance begins producing candidate pairs */
	if (parameters.readAllRecordsBeforeCompare && false == linkage){
		cout << " All records read, begining block generation\n";
		int blockId = 0;
#ifdef USE_FASTTRIE
#else
		for(vector<BlocksMap>::iterator it = blocks.begin(); it != blocks.end(); it++){
			BlocksMap::iterator it2;
			BlocksMap blockMap = (*it);
			for(it2 = blockMap.begin(); it2 != blockMap.end(); it2++){
				ListOfRecordIds *listOfIds = (*it2).second;

				ListOfRecordIds::const_iterator itIds = listOfIds->begin();
				ListOfRecordIds::const_iterator itLast = listOfIds->end();

				int recnum = *itIds;
				itIds ++; //First element doesn't form a par with itself!

				for(;;) {
					if (itIds != itLast){
						GenerateCombinations(itIds, itLast, recnum, blockId,
							&mergerMessage);
						//cout << "Gerou " << recnum << " " << *(itIds) << endl;
						recnum = *itIds;
						++ itIds;
					} else {
						break;
					}
				}
			}
			blockId ++;
		}
#endif
	}
	/*
	 * Sends remaining msgs(if exist) (maybe the counter did not reach
	 * the value MAX_PAIRS_TO_MERGER)
	 */
	mergerMessage.Flush();

	int *duplicated = mergerMessage.getDuplicatedPairs();
	for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
		this->totalOfDuplicatedPairs += duplicated[i];
	}
	/*
	 * Print statistics.
	 */
	LOGFPINFO(logger, "Received: " + Util::ToString(totalOfBlockingMsgReceived) + " Total of pairs: "
			+ Util::ToString(mergerMessage.getTotalOfPairs())
			+ " duplicated: " + Util::ToString(mergerMessage.getTotalOfDuplicatedPairs()));
	/* FIXME: These operations take long time to run and may be more practical generate the histogram from the results.
	BlocksMap::iterator it2;
	if (logger->IsLevelEnabled(Feraparda::LOGSTAT)) {
		int *duplicated = mergerMessage.getDuplicatedPairs();
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			if (blocksGenerated[i]) {
				LOGFPSTAT(logger,
						"@@Instancia( " + Util::ToString(myRank) + " ): Stats for conjunction " +
						Util::ToString(i) + ": Blocks: " +
						Util::ToString(blocksGenerated[i]) +
						" Pairs: " + Util::ToString(pairsGenerated[i]) +
						" Duplicated: " + Util::ToString(duplicated[i]));
			}
		}
	}
	if (logger->IsLevelEnabled(Feraparda::LOGDEBUG)) {
		//Dump histogram of blocks
		int i = 0;
		for (it2 = blocks.begin(); it2 != blocks.end(); ++it2) {
			LOGFPDEBUG(logger, "Conjunction " + it2->first.first +
			 " Total " + Util::ToString((int)it2->second->size()) +
			 " Key: " + it2->first.second);

			i++;
		}
	}*/
#ifndef USE_FASTTRIE
	for(vector<BlocksMap>::iterator it = blocks.begin(); it != blocks.end(); it++){
		BlocksMap blocks = (*it);
		BlocksMap::iterator it2 = blocks.begin();
		while (it2 != blocks.end()) {
			//char* key = it2->first;

			/* Delete the list of ids. The blocking key is deleted by the STL map */
			delete it2->second;

			blocks.erase(it2);
			it2 = blocks.begin();
		}
	}
#endif
}
/**
 * Forms the combinations.
 */
void BlockingFilter::GenerateCombinations(
		ListOfRecordIds::const_iterator start, ListOfRecordIds::const_iterator end,
		int idRecord, unsigned char blockId, MergerMessage *mergerMessage) {

	ListOfRecordIds::const_iterator it;
	record_pair_msg_t outMsg;

	for (it = start; it != end; ++it) {

		int id = *it;

		//Smaller id always goes first.
		if (idRecord < id) {
			outMsg.id1 = idRecord;
			outMsg.id2 = id;
		} else {
			outMsg.id1 = id;
			outMsg.id2 = idRecord;
		}
		outMsg.blockId = blockId;

		if (logger->IsLevelEnabled(LOGDEBUG)) {
			ostringstream sout;
			sout << "Pair (" << outMsg.id1 << "," << outMsg.id2
					<< "), blockId: " << (int) outMsg.blockId;
			LOGFPDEBUG(logger, sout.str());
		}

		this->pairsGenerated[blockId]++;

		if (logger->IsLevelEnabled(LOGDEBUG) && pairsGenerated[blockId] % 10000
				== 0) {
			LOGFPDEBUG(logger, "Pairs for blocking " + Util::ToString(blockId) + ": " +
					Util::ToString(pairsGenerated[blockId]));
		}
		mergerMessage->Send(outMsg);
	}
}
void MergerMessage::ConfigureNeedAck(record_pair_grouped_msg_t group, bool needsAck){
	throw FerapardaException("Message does not support ACK.");
}
bool MergerMessage::Add(record_pair_msg_t msg) {
	KeyPair hashKey;
	hashKey.id1 = msg.id1;
	hashKey.id2 = msg.id2;
	//cout << " Blocking " << msg.id1 << " " << msg.id2 << endl;

	if (msg.id1 < msg.id2) { // Hack inserted to ensure the order 15/07/08
		hashKey.id1 = msg.id1; //TODO: Review if the BI was right, it is generating pars
		hashKey.id2 = msg.id2;
	} else {
		hashKey.id1 = msg.id2;
		hashKey.id2 = msg.id1;
	}

	assert(hashKey.id1 < hashKey.id2);

	if (this->sentPairs.find(hashKey) == this->sentPairs.end()) {
		this->lastGroup = msg.id2 % this->totalOfTargets;
		this->groups[lastGroup].msgs[counter[lastGroup]] = msg;
		this->counter[lastGroup]++;
		this->groups[lastGroup].total++;
		totalOfPairs++;
		if ((totalOfPairs % 1000000) == 0){
			cout << totalOfPairs << " pairs generated at blocking filter #" << ahUtil->Rank() << endl;
		}

		if (totalOfPairs >= DISTINCT_QUEUE_SIZE) {
			this->sentPairs.erase(
					circularQueueSentPairs[positionInCircularQueue]);
		}
		//cout << " Add " << hashKey.id1 << "," << hashKey.id2 << endl ;
		circularQueueSentPairs[positionInCircularQueue].id1 = msg.id1;
		circularQueueSentPairs[positionInCircularQueue].id2 = msg.id2;
		positionInCircularQueue = (positionInCircularQueue + 1)
				% DISTINCT_QUEUE_SIZE;
		this->sentPairs.insert(hashKey);
		return (counter[lastGroup] >= this->max);
	} else {
		//ignore this pair, because it had already been processed
		duplicatedPairs[msg.blockId]++;
		return false;
	}
	return false;
}

#ifndef _READER_H
#define _READER_H

#include "../project.h"
#include "../messagesfp.h"
#include "../groupedmessage.h"
#include "../basefilter.h"
#include "../anthillutil.h"
#include "ResultMessage.h"

#include <list>
using namespace std;
using namespace Feraparda;

namespace Feraparda {

/**
 * A grouped blocking message handler.
 */
class BlockingMessage : public GroupedMessage<block_msg_t, block_grouped_msg_t> {
protected:
	virtual bool Add(block_msg_t msg);
	virtual void ConfigureNeedAck(block_grouped_msg_t group, bool needsAck);

public:
	BlockingMessage(AnthillUtil *ahUtil, AnthillOutputPort out, int totalOfTargets, int max) :
		GroupedMessage<block_msg_t, block_grouped_msg_t>(
				ahUtil,out, totalOfTargets, max) {
		for (int i = 0; i < totalOfTargets; i++) {
			groups[i].mod = i;
			groups[i].total = 0;
		}
	}
};
/**
 * A pair message handler.
 */
class ComparePairMessage : public GroupedMessage<compare_pair_msg_t, compare_pair_grouped_msg_t> {
private:
	int totalOfUseCacheMessages;
	int totalOfRecordMessages;
protected:
    virtual bool Add(compare_pair_msg_t msg){return false;}	
    virtual void ConfigureNeedAck(compare_pair_grouped_msg_t group, bool needsAck){
    	throw FerapardaException("Message does not support ACK.");
    }
	virtual bool Add(record_msg_t msg);
	virtual bool Add(use_cache_msg_t msg);
public:
	virtual void Flush(int);
	virtual void Flush();
	int getTotalOfUseCacheMessages(){return totalOfUseCacheMessages;}
	int getTotalOfRecordMessages(){return totalOfRecordMessages;}

	void Send(record_msg_t msg){
	  totalOfRecordMessages ++;
	  Add(msg); 
	}
	void Send(use_cache_msg_t msg){
	  totalOfUseCacheMessages ++;
	  Add(msg); 
	}
	ComparePairMessage(AnthillUtil *ahUtil, AnthillOutputPort out, int totalOfTargets, int max) :
		GroupedMessage<compare_pair_msg_t, compare_pair_grouped_msg_t>(
				ahUtil, out, totalOfTargets, max) {
		for (int i = 0; i < totalOfTargets; i++) {
			groups[i].mod = i;
			groups[i].total = 0;
			groups[i].source = ahUtil->Rank();
		}
		totalOfRecordMessages = 0;
		totalOfUseCacheMessages = 0;
	}
};

/**
 * Filter definition.
 */
class ReaderFilter : public FerapardaBaseFilter{
public:
	~ReaderFilter();
	void Process();
	int getRank() {
		return myRank;
	}
	ReaderFilter(AnthillUtil *ahUtil, void*);
	bool IsRecordMine(int);
	Record *GetRecordUsingInternalId(int);
private:
	void SendRecordToBlocking(int id, Record *record, bool dataSource1);
	void ProcessDeterministic();
	void ProcessLinkage();
	void ProcessDeduplication();
	void Compare(result_msg_t *, Record *, Record *, unsigned int,
				unsigned int, unsigned int);

	void ReceiveAndProcessMessages();
	void ReceiveAndProcessCompareMessage();
	void ReceiveAndProcessCompareMessageFromOtherInstances();
	void ReceiveAndProcessUseCacheMessage();
	void ReceiveAndProcessReceiveRecordMessage();

	void SendKeysToBlocking(int, vector<string>, short dataSource);
	void SendRecordToReader(Record *record, unsigned int, unsigned int,
			unsigned int);
	void SendUseCacheToReader(unsigned int, unsigned int, unsigned int);

	void OpenOutputPorts();
	void OpenInputPorts();
	void ReadCandidatePairs();
	void CompareMessage(record_pair_msg_t msg);

private:
	hash_map<int,list<int> * > waitingRecord;
	int dataSource0MaxId;
	int useCacheOutOpened;
	int recordOutOpened;
	int overhead;
	bool linkage;
	//Statistics
	unsigned long totalOfBothRecordsOwned;
	DataSource *smallest;
	DataSource *largest;
	int totalOfMissedRecords;
	//controls sent records' cache
	Cache<unsigned int, int *> *idsSentRecords;
	Cache<unsigned int, Record *> *receivedRecords;
	//
		/*
	 * Note: Port names consider the point of view of this filter
	 */
	/* Anthill communication ports */
	AnthillOutputPort blockerOutput;
	AnthillInputPort mergerInput;
	AnthillInputPort blockerInput;

	AnthillOutputPort useCacheOutput;
	AnthillInputPort useCacheInput;

	AnthillOutputPort recordOutput;
	AnthillInputPort recordInput;

	AnthillOutputPort resultOutput;

	bool recordPairInputOpened;
	bool recordInputOpened;
	bool useCacheInputOpened;

	int totalOfReaderInstances;
	int totalOfBlockerInstances;
	int totalOfResultInstances;
	int myRank;
	int startRecordNumberInDs0; //first record read by this instance in data source 0
	int startRecordNumberInDs1; //first record read by this instance in data source 1
	Project *project;
	string projectConfig;
	BlockingMessage *blockingMessage;
	ResultMessage *resultMessage;
	ComparePairMessage *comparePairMessage;
	ComparePairMessage *deterministicBlockingMessage;
	int totalOfRecordsReceived;
	int totalOfUseCacheMetaReceived;
};
}
#endif


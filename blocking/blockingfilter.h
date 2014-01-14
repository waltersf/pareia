#ifndef _BLOCKING_H
#define _BLOCKING_H

#include "messagesfp.h"
#include "groupedmessage.h"
#include "stlutil.h"
#include "basefilter.h"
#include "anthillutil.h"
#include <set>
#include <map>
#include <ext/hash_map>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#ifdef USE_FASTTRIE
#include "../trie/fasttrie.h"
#endif
using namespace std;
using namespace __gnu_cxx;

class ByteAlignedIntegerCollection;
typedef std::vector<int> ListOfRecordIds;


#ifdef USE_FASTTRIE
typedef jdk_fasttree<ListOfRecordIds *, 128, char > BlocksMap;
#else
typedef map<const char*, ListOfRecordIds*, BlockKeyComparator > BlocksMap;
#endif

typedef hash_map<std::string, std::string, stu_comparator_t, stu_eqstr_t>
		BlocksIdsMap;

namespace Feraparda {
/**
 * Group messages sent to Merger filter, optimizing network utilization.
 */
class MergerMessage: public GroupedMessage<record_pair_msg_t,
		record_pair_grouped_msg_t> {
private:
	/** Stores the sent pairs and prevent processing them again*/
	set<KeyPair, PairComparator> sentPairs;
	/** The maximum number of blocking conjunctions */
	KeyPair circularQueueSentPairs[DISTINCT_QUEUE_SIZE];
	int positionInCircularQueue;
	int *duplicatedPairs;
	long totalOfPairs;
	bool lastSortWasUsingLessThan;
	bool sortAscending; //option used to order the messages
protected:
	virtual bool Add(record_pair_msg_t msg);
	virtual void ConfigureNeedAck(record_pair_grouped_msg_t group, bool needsAck);
public:
	int getTotalOfPairs() {
		return totalOfPairs;
	}
	int getTotalOfDuplicatedPairs() {
		int total = 0;
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			total += duplicatedPairs[i];
		}
		return total;
	}

	int *getDuplicatedPairs() {
		return duplicatedPairs;
	}
	~MergerMessage() {
		delete[] duplicatedPairs;
	}
	/**
	 */
	MergerMessage(AnthillUtil *ahUtil, AnthillOutputPort out,
			int totalOfTargets, int max) :
		GroupedMessage<record_pair_msg_t, record_pair_grouped_msg_t> (ahUtil,
				out, totalOfTargets, max) {

		this->totalOfTargets = totalOfTargets;
		duplicatedPairs = new int[MAX_BLOCK_TYPES];
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			duplicatedPairs[i] = 0;
		}
		for (int i = 0; i < this->totalOfTargets; i++) {
			groups[i].mod = i;
			groups[i].total = 0;
		}
		totalOfPairs = 0;
		sortAscending = true;
		positionInCircularQueue = 0;
	}
};
class BlockingFilter: public FerapardaBaseFilter {
public:
	~BlockingFilter() {
		delete[] pairsGenerated;
		delete[] blocksGenerated;
	}
	void Process();
	void ProcessDeterministicBlocking();
	void ProcessBlocking();

	BlockingFilter(AnthillUtil *util, void *);
	int GetTotalOfPairs() {
		int total = 0;
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			total += pairsGenerated[i];
		}
		return total;
	}
	int GetTotalOfBlocks() {
		int total = 0;
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			total += blocksGenerated[i];
		}
		return total;
	}
	int GetTotalOfBlockingMessagesReceived(){
		return totalOfBlockingFilterInstances;
	}
	int GetTotalOfDuplicatedPairs(){
		return totalOfDuplicatedPairs;
	}
private:
	int myRank;
	int totalOfBlockingFilterInstances;
	int *blocksGenerated;
	int *pairsGenerated;
	int totalOfBlockingMsgReceived;
	int totalOfDuplicatedPairs;
	int totalInstancesOfMerger;

	void SendPairToMerger(int, int, int);
	void GenerateCombinations(ListOfRecordIds::const_iterator,  ListOfRecordIds::const_iterator, int, unsigned char,
			MergerMessage *);
	void OpenPorts();
	AnthillOutputPort out;
	AnthillOutputPort resultOut;
	AnthillInputPort in;

};
}
class ByteAlignedIntegerCollection {
private:
	vector<unsigned char> store;
	int previousInEnum;
	int lastInserted;
	int total;
	int current;
public:
	void Add(int number){
		unsigned char first = 0;
		int delta = number - lastInserted;
		unsigned bytesNeeded = 1;
		if (delta < 0){
			delta *= -1;
			first |= 0x80; //set signal flag as negative offset
		}
		if (delta <= 15){
			first |= 0x0; //Flag as just one byte needed
			first |= delta;
			store.push_back(first);
		} else {
			first |= 15;
			delta -= 15;
			store.push_back(first);
			int pos = store.size() - 1;

			while (delta > 0){
				store.push_back(delta & 255);
				delta = delta >> 8;
				bytesNeeded ++;
			}
			store[pos] |= (bytesNeeded -1) << 4;
			if (bytesNeeded > 5){
				cout << lastInserted << " " << number << endl << flush;
			}
			assert(bytesNeeded <=5);
		}
		total ++;
		lastInserted = number;
	}
	void MoveToStart(){
		current = 0;
		previousInEnum = 0;
	}
	int GetNext() {
		unsigned char first = store[current ++];
		char signal = (first & 0x80) >> 7; //1000 0000
		int bytes = ((first & 0x70) >> 4); //0110 0000
		int offset = first & 0x0F; //0000 1111

		int shift = 0;
		while (bytes --){
			offset += (store[current ++] << shift);
			shift += 8;
		}

		previousInEnum = (signal?-1:1) * offset + previousInEnum;
		return previousInEnum;
	}
	int GetTotal(){
		return total;
	}
	ByteAlignedIntegerCollection(){
		lastInserted = 0;
		MoveToStart();
	}
	int GetTotalOfElementsInVector(){
		return store.size();
	}
	void Clear(){
		store.clear();
		MoveToStart();
		total = 0;
		lastInserted = 0;
	}
};
#endif

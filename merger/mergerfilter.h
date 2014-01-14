#ifndef _MERGER_H
#define _MERGER_H

#include "messagesfp.h"
#include "groupedmessage.h"
#include "stlutil.h"
#include "project.h"
#include "anthillutil.h"
#include "basefilter.h"
#include "schedulerheuristic.h"
#include <zlib.h>
#include <list>
#include <set>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;
using namespace __gnu_cxx;

struct heuristic_grouped_msg_t {
	record_pair_msg_t *msgs;
	int total;
	int mod;
};

namespace Feraparda {
class CompareMessage: public GroupedMessage<record_pair_msg_t,
		record_pair_grouped_msg_t> {
private:
	/** Stores the sent pairs and prevent processing them again*/
	set<KeyPair, PairComparator> sentPairs;
	int *duplicatedPairs;
	int totalOfSources;
	int totalOfTargetInstances;
	int totalOfPairs;
	bool useHeuristic;
	bool sendPairsUsingSmallerId;
	bool Include(record_pair_msg_t msg, KeyPair hashKey);
	bool Include(record_pair_msg_t msg, KeyPair hashKey, bool notUseHeuristic,
			bool force, bool sendPairsUsingSmallerId);
	int maxHeuristicGraphSize;
	heuristic_grouped_msg_t *groupsHeuristic;
	GraphHeuristic *heuristic;
	void SendPairs(InfoAdjacent *, GraphHeuristic *);
protected:
	virtual bool Add(record_pair_msg_t msg);
	virtual void ConfigureNeedAck(record_pair_grouped_msg_t group, bool needsAck);
	void FlushHeuristic(int, bool);
public:
	void setSendPairsUsingSmallerId(bool s) {
		sendPairsUsingSmallerId = s;
	}
	void setUseHeuristic(bool v) {
		useHeuristic = v;
	}
	virtual void Flush();
	void Flush(int target);
	int getTotalOfPairs() {
		return totalOfPairs;
	}
	int *getDuplicatedPairs() {
		return duplicatedPairs;
	}
	/**
	 */
	CompareMessage(AnthillUtil *ahUtil, AnthillOutputPort out,
			int totalOfTargetInstances, int totalOfSources, int max,
			int graphSize) :
		GroupedMessage<record_pair_msg_t, record_pair_grouped_msg_t> (ahUtil,
				out, totalOfTargetInstances, max) {

		this->totalOfSources = totalOfSources;
		this->totalOfTargetInstances = totalOfTargetInstances;
		duplicatedPairs = new int[MAX_BLOCK_TYPES];
		for (int i = 0; i < MAX_BLOCK_TYPES; i++) {
			duplicatedPairs[i] = 0;
		}

		for (int i = 0; i < totalOfTargetInstances; i++) {
			groups[i].mod = i;
			groups[i].total = 0;
		}
		int total = (1 + totalOfTargetInstances) * totalOfTargetInstances / 2;
		groupsHeuristic = new heuristic_grouped_msg_t[total];
		for (int i = 0; i < total; i++) {
			groupsHeuristic[i].mod = i;
			groupsHeuristic[i].msgs = new record_pair_msg_t[graphSize];
			groupsHeuristic[i].total = 0;
		}
		totalOfPairs = 0;
		this->maxHeuristicGraphSize = graphSize;

		heuristic = new GraphHeuristic[total];
	}
	~CompareMessage() {
		delete[] duplicatedPairs;
		int total = (1 + totalOfTargetInstances) * totalOfTargetInstances / 2;
		for (int i = 0; i < total; i++) {
			delete[] groupsHeuristic[i].msgs;
		}
		delete[] groupsHeuristic;
		delete[] heuristic;
	}
};
class MergerFilter: public FerapardaBaseFilter {
public:
	~MergerFilter() {
	}
	void Process();
	MergerFilter(AnthillUtil *ahUtil, void *);
	void setUseHeuristic(bool v) {
		useHeuristic = v;
	}
	void setSendPairsUsingSmallerId(bool s) {
		sendPairsUsingSmallerId = s;
	}
	int getPairsGenerated() {
		return pairsGenerated;
	}
private:
	Project *project;
	int myRank;
	bool stopAtThisStage;
	bool useHeuristic;
	bool sendPairsUsingSmallerId;
	int pairsGenerated;

	int totalInstancesOfReader;
	int totalInstancesOfBlocking;

	void SendPairToMerger(int, int, int);

	void OpenPorts();
	AnthillOutputPort out;
	AnthillInputPort in;
};
}
#endif

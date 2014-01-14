/**
 * mergerfilter.c
 * (c) e-Speed/DCC/UFMG
 * (c) Walter dos Santos Filho - waltersf AT gmail.com
 */
#include "mergerfilter.h"
#include "groupedmessage.h"
#include "logging.h"
#include "stlutil.h"
#include "anthillutil.h"
#include "schedulerheuristic.h"
#include "project.h"
#include <cassert>
#include <fcntl.h>
#include <zlib.h>
using namespace std;
using namespace Feraparda;
CREATE_ANTHILL_FILTER (MergerFilter, filter, new AnthillUtil())
;

MergerFilter::MergerFilter(AnthillUtil *ahUtil, void* data) {

	this->project = (Project *) data;
	this->ahUtil = ahUtil;

	this->logger = LogFactory::GetLogger("merger");

	this->stopAtThisStage = parameters.stopAtMerging;

	this->setUseHeuristic(parameters.useHeuristic);
	this->setSendPairsUsingSmallerId(parameters.useSmallerId);

	this->myRank = ahUtil->Rank();
	OpenPorts();
	this->totalInstancesOfReader = ahUtil->TotalOfReaders(out);
	this->totalInstancesOfBlocking = ahUtil->TotalOfWriters(in);
	this->pairsGenerated = 0;
}
void MergerFilter::OpenPorts() {
	in = ahUtil->GetInputPort("blockerInput");
	out = ahUtil->GetOutputPort("readerOutput");
}
void MergerFilter::Process() {
	record_pair_grouped_msg_t msg;
	if (this->stopAtThisStage) {
#ifdef LOGDEBUG
		if (logger->IsLevelEnabled(LOGDEBUG)) {
			ostringstream o;
			o << "Candidate pairs will be written to file " << project->getOutput()->getOutput();
			LOGFPDEBUG(logger, o.str());
		}
#endif
		int fd = open64(project->getOutput()->getCandidates().c_str(), O_WRONLY | O_CREAT , 0666);
		if (fd < 0){
			throw FerapardaException(string("Error opening file (") +
					project->getOutput()->getCandidates() +
					string(") for outputing candidate pairs at merger filter: ") + string(strerror(errno)));
		}
		gzFile outFile = gzdopen(fd, "wb");
		while (ahUtil->Read(in, &msg, sizeof(record_pair_grouped_msg_t)) != EOW) {
			for (int i = 0; i < msg.total; i++) {
				gzprintf(outFile, "%d %d %d\n", msg.msgs[i].id1, msg.msgs[i].id2, msg.msgs[i].blockId);
			}
		}
		gzseek(outFile, 1L, SEEK_CUR); // add one zero byte
		gzclose(outFile);
	} else {
		CompareMessage compareMessage(ahUtil, out, totalInstancesOfReader,
				totalInstancesOfBlocking, GROUP_SIZE_MSG_PAIR, 5000);
		compareMessage.setUseHeuristic(this->useHeuristic);
		compareMessage.setSendPairsUsingSmallerId(this->sendPairsUsingSmallerId);

		if (!parameters.startAtMerging) {
			while (ahUtil->Read(in, &msg, sizeof(record_pair_grouped_msg_t)) != EOW) {
				pairsGenerated += msg.total;
				for (int i = 0; i < msg.total; i++) {
					//the distinctness is done in the ReaderMessage::Add() method
					compareMessage.Send(msg.msgs[i]);
				}
			}
		} else {
			/*
			char buf[4096];
			int err;

			int fd = open64(project->getOutput()->getCandidates().c_str(), O_RDONLY, 0666);
			if (fd < 0){
			throw FerapardaException(string("Error opening file (") +
					project->getOutput()->getCandidates() +
					string(") for outputing candidate pairs: ") + string(strerror(errno)));
			}
			gzFile outFile = gzdopen(fd, "r");
			if (inFile == NULL){
				throw FerapardaException("File not found when reading candidate pairs at Merger Filter");
			}
			record_pair_msg_t msg;
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
		        	//FIXME: Como fazer isto em C++? Utilizar istringstream?
		        	sscanf(buf, "%d %d %d", &id1, &id2, &block);
		        	msg.id1 = id1;
		        	msg.id2 = id2;
		        	msg.blockId = block;
		        	compareMessage.Send(msg);
		        }
		    }
			gzclose(inFile);
			*/
		}
		/*
		 * Sends remaining msgs(if exists), this is used in the case that the msgs
		 * in each buffer did not reached the limit (GROUP_SIZE_MSG_PAIR)
		 * */
		compareMessage.Flush();
		if (logger->IsLevelEnabled(Feraparda::LOGINFO)) {
			int *duplicated = compareMessage.getDuplicatedPairs();
			for (int i = 0; i < MAX_BLOCK_TYPES; ++i) {
				if (duplicated[i]) {
					LOGFPINFO(logger, "Duplicated pairs for conjunction " +
							Util::ToString(i) + ": " + Util::ToString(duplicated[i]));
				}
			}
			LOGFPINFO(logger, "Total of pairs: " + Util::ToString(compareMessage.getTotalOfPairs()));
		}
	}
}
void CompareMessage::ConfigureNeedAck(record_pair_grouped_msg_t group, bool needsAck){
	throw FerapardaException("Message does not support ACK.");
}
bool CompareMessage::Add(record_pair_msg_t msg) {

	KeyPair hashKey;
	hashKey.id1 = msg.id1;
	hashKey.id2 = msg.id2;
	/*
	 * Check if it can ignore the pair, because it had already been processed.
	 * If there is only 1 instance of source (blocking filter), the merger does
	 * not have to perform the distinct operation, because it was done in 
	 * previous stage.
	 */
	if (this->totalOfSources > 1 || parameters.startAtMerging) {

		//Never sent before?
		if (this->sentPairs.find(hashKey) == this->sentPairs.end()) {
			//Limit reached? Y: Dequeue
			if (this->sentPairs.size() >= DISTINCT_QUEUE_SIZE) {
				this->sentPairs.erase(this->sentPairs.begin());
			}
			this->sentPairs.insert(hashKey);
			return Include(msg, hashKey, !this->useHeuristic, false,
					this->sendPairsUsingSmallerId);
		} else {
			duplicatedPairs[msg.blockId]++;
			return false;
		}
	} else {
		//Just forward the message
		return Include(msg, hashKey, !this->useHeuristic, false,
				this->sendPairsUsingSmallerId);
	}
}

void CompareMessage::Flush(int target) {
	GroupedMessage<record_pair_msg_t, record_pair_grouped_msg_t>::Flush(target);
	return;
}
void CompareMessage::Flush() {
	if (useHeuristic) {
		for (int i = 0; i < (totalOfTargetInstances + 1)
				* totalOfTargetInstances / 2; i++) {
			FlushHeuristic(i, true);
		}
	}
	GroupedMessage<record_pair_msg_t, record_pair_grouped_msg_t>::Flush();
	return;
}
bool CompareMessage::Include(record_pair_msg_t msg, KeyPair hashKey) {
	//int target = (msg.id2 > msg.id1 ? msg.id2 : msg.id1) % this->totalOfTargetInstances;
	int target = msg.id2 % this->totalOfTargetInstances;
	this->lastGroup = target;
	this->groups[target].msgs[counter[target]] = msg;
	this->groups[target].total++;
	this->groups[target].mod = (char) target;
	this->counter[target]++;
//	cout << ">>>>>>>>>>>>>>>> Pair (" << hashKey.id1 << "," << hashKey.id2 << ") Target: " << target << endl;
#ifdef LOGDEBUG		
	if (logger->IsLevelEnabled(LOGDEBUG)) {
		ostringstream o;
		o << "Pair (" << hashKey.id1 << "," << hashKey.id2 << ") Target: " << target;
		LOGFPDEBUG(logger, o.str());
	}
#endif		
	this->totalOfPairs++;
	return (counter[lastGroup] >= this->max);

}
bool CompareMessage::Include(record_pair_msg_t msg, KeyPair hashKey,
		bool notUseHeuristic, bool force, bool sendPairsUsingSmallerId) {
	/*
	 * This is an optimization. Theorically, the reader that generated the 
	 * bigger id is beyond the other one and can process the pair. Doing 
	 * so, the reader instances will be better balanced.
	 */
	int target;
	if (notUseHeuristic) {
		if (sendPairsUsingSmallerId) {
			if (msg.id1 < msg.id2) {
				int temp = msg.id2;
				msg.id2 = msg.id1;
				msg.id1 = temp;
			}
		} else {
			if (msg.id1 > msg.id2) {
				int temp = msg.id2;
				msg.id2 = msg.id1;
				msg.id1 = temp;
			}
		}
		return Include(msg, hashKey);
	} else {
		int targetId1 = msg.id1 % this->totalOfTargetInstances;
		int targetId2 = msg.id2 % this->totalOfTargetInstances;
		if (targetId1 == targetId2) {
			return Include(msg, hashKey);
		}
		if (targetId1 > targetId2) {
			int aux = targetId1;
			targetId1 = targetId2;
			targetId2 = aux;
		}
		/*
		 * There is a queue for each pair of targets. In this case, the order
		 * is not important, so, the pair 0-1 is represented and 1-0 not.
		 * The formula below calculate the final position in the vector of queues.
		 * For example, using 3 instances, we'd have queues (0,0), (0,1), (0,2), (0, 3),
		 * (1, 1), (1, 2), (1,3), (2, 2), (2,3) and (3,3).
		 */
		target = targetId1 * totalOfTargetInstances + targetId2 - (targetId1
				* (targetId1 + 1) / 2);

		this->groupsHeuristic[target].msgs[groupsHeuristic[target].total] = msg;
		this->groupsHeuristic[target].total++;
		this->groupsHeuristic[target].mod = (char) target;

		if (this->groupsHeuristic[target].total >= maxHeuristicGraphSize) {
			FlushHeuristic(target, false);
		}
		return false;
	}
}
void CompareMessage::FlushHeuristic(int target, bool force) {
	heuristic_grouped_msg_t *group = &(this->groupsHeuristic[target]);
	if (group->total == 0) {
		return;
	}
	/*
	 * Copies the pairs to the graph.
	 */
	for (int i = 0; i < group->total; i++) {
		//Exchange the order of ids because other parts of algorith (reader filter) needs.
		if (group->msgs[i].id1 > group->msgs[i].id2) {
			int aux = group->msgs[i].id1;
			group->msgs[i].id1 = group->msgs[i].id2;
			group->msgs[i].id2 = aux;
		}
		heuristic[target].AddPair(group->msgs[i]);
	}
	if (force) {
		heuristic[target].Sort();
		//int commCost = 0;
		int ratio = 32;
		//int flushed = 0;
		while (ratio > 0) {
			InfoAdjacent *info =
					heuristic[target].RemoveVertexWithLargestDegree();
			//InfoAdjacent *info = heuristic[target].RemoveNextWithDegreeGt(ratio);
			while (info != NULL) {
				SendPairs(info, &heuristic[target]);
				//commCost ++;
				//flushed += info->total;
				delete info;
				info = heuristic[target].RemoveVertexWithLargestDegree();
				//info = heuristic[target].RemoveNextWithDegreeGt(ratio);
			}
			ratio /= 2;
		}
		//cout << "Forced flushed " << flushed << " with cost " << commCost << endl;

	} else {
		//int commCost = 0;
		int toRemove = group->total;
		//cout << "Flushing merger .............. " << " " << toRemove << "/" <<  this->groupsHeuristic[target].total  << endl;
		//int flushed = toRemove;
		int ratio = 32;
		heuristic[target].Sort();
		while (toRemove > 0) {
			InfoAdjacent *info =
					heuristic[target].RemoveVertexWithLargestDegree();
			//InfoAdjacent *info = heuristic[target].RemoveNextWithDegreeGt(ratio);
			while (info != NULL) {
				SendPairs(info, &heuristic[target]);
				toRemove -= info->total;
				//commCost ++;
				info->list->clear();
				delete info;
				if (toRemove <= 0) {
					break;
				}
				info = heuristic[target].RemoveVertexWithLargestDegree();
				//info = heuristic[target].RemoveNextWithDegreeGt(ratio);
			}
			ratio /= 2;
		}
		//flushed -= toRemove;
		//cout << "Flushed " << flushed << " with cost " << commCost << endl;
	}
	group->total = 0;
}
void CompareMessage::SendPairs(InfoAdjacent *info, GraphHeuristic *heuristic) {
	unsigned int id2 = (unsigned) info->id;
	ListOfRecordIds::iterator it;
	for (it = info->list->begin(); it != info->list->end(); ++it) {
		InfoAdjacent *terminal = heuristic->GetVertex((*it).first);
		//if (terminal != NULL && (terminal->total < info->total ||(terminal->total == info->total && terminal->id < info->id) )){
		int finalTarget = id2 % totalOfTargetInstances;
		record_pair_msg_t *msg =
				&(this->groups[finalTarget].msgs[counter[finalTarget]]);
		msg->id1 = (*it).first;
		msg->id2 = id2;
		this->counter[finalTarget]++;
		this->groups[finalTarget].total++;
		assert(this->groups[finalTarget].total <= max);
		if (this->counter[finalTarget] >= this->max) {
			Flush(finalTarget);
		}
		terminal->list->erase(id2);
		//}
	}
}

ostream& operator<<(ostream& os, const record_pair_msg_t& msg) {
	return os << "(" << msg.id1 << ',' << msg.id2 << ") - ";
}
bool operator <(const record_pair_msg_t& a, const record_pair_msg_t& b) {
	return (a.id2 == b.id2) ? (a.id1 < b.id1) : (a.id2 < b.id2);
}
bool sortAsc(const record_pair_msg_t& a, const record_pair_msg_t& b) {
	return (a.id2 == b.id2) ? (a.id1 < b.id1) : (a.id2 < b.id2);
}
bool sortDesc(const record_pair_msg_t& a, const record_pair_msg_t& b) {
	return (a.id2 == b.id2) ? (b.id1 < a.id1) : (b.id2 < a.id2);
}
bool sortByDegree(ListEntry a, ListEntry b) {
	return (a.info->total < b.info->total);
}

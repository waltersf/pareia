#ifndef _GRAPH_HEURISTIC_H_
#define _GRAPH_HEURISTIC_H_

#include <sstream>
#include <list>
#include <map>
#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include "messagesfp.h"
#include <assert.h>
using namespace std;
using namespace __gnu_cxx;



namespace Feraparda {
  typedef std::map<int, char> ListOfRecordIds;
  class InfoAdjacent {
			public:
		    int total;             //because list.size() may be O(N)
				int id;
				ListOfRecordIds *list;
				InfoAdjacent(){
						list = new ListOfRecordIds();
				}
				~InfoAdjacent(){
						list->clear();
						delete list;
				}
  };
struct ListEntry {
	InfoAdjacent *info;
	bool operator <(const ListEntry& other) {
		  return (this->info->total < other.info->total);
	}
};
 typedef vector<ListEntry> DegreeOrderedTAD;
  typedef map<int, ListEntry> PairGraph;

  class GraphHeuristic{
  private:
		void InternAddPair(int id1, int id2){
				assert(id1 != id2);
				ListEntry entry;

				PairGraph::iterator graphIterator;
				if ((graphIterator = graph.find(id1)) == graph.end()){
						//new element
						entry.info = new InfoAdjacent();
						entry.info->total = 1;
						entry.info->id = id1;
						listOfVertex.push_back(entry);
						graph[id1] = entry; 
						entry.info->list->insert(pair<int,char>(id2, '1'));
				} else {
						//already exists
						entry = graphIterator->second;
						entry.info->total ++;
						entry.info->list->insert(pair<int,char>(id2, '1'));
				}
		}
	public:
	  void Sort(){
	  	make_heap(listOfVertex.begin(), listOfVertex.end());
	  	sort_heap(listOfVertex.begin(), listOfVertex.end());
		//sort(listOfVertex.begin(), listOfVertex.end(), sortByDegree);
		sorted = true;
	  }
	  GraphHeuristic(){
				selectedForRemove = graph.begin(); 
				sorted = false;
	  }
	  ~GraphHeuristic(){
				PairGraph::iterator it = graph.begin();
	  }
		void AddPair(record_pair_msg_t msg){
			sorted = false;
				assert(msg.id1 != msg.id2);
				InternAddPair(msg.id1, msg.id2);
				InternAddPair(msg.id2, msg.id1);
		}

	  /*
	   * Dumps the graph
	   */
		void Dump(){
				PairGraph::iterator it;
				for(it = graph.begin(); it != graph.end(); ++it){
						//cout << it->first << " has " << it->second->total << " neighbors" << endl;
				}
		}
		/*
		InfoAdjacent *RemoveNextWithDegreeGt(int degree){
				InfoAdjacent *info  = NULL;
				if (graph.empty()){
						return NULL;
				} else if (graph.size() == 1){
						info = graph.begin()->second;
						graph.clear();
						return info;
				} else { 
						if (selectedForRemove == graph.end()){
								selectedForRemove = graph.begin();
						}
						PairGraph::iterator start = selectedForRemove;
						do {
								if (selectedForRemove->second->total >= degree){
										info = selectedForRemove->second;
										ListOfRecordIds::iterator itList;
										for(itList = info->list->begin(); itList != info->list->end(); ++itList){
										  if (graph.find(*itList) != graph.end()){ //nao deveria ser necessario
											InfoAdjacent *adjacent = graph[*itList];
											if (adjacent != NULL){
											  if(adjacent->total == 1){
												graph.erase(*itList);
												delete adjacent;
											  } else {
												adjacent->list->remove(info->id);
												adjacent->total --;
											  }
											}
										  }
										}
										PairGraph::iterator it = selectedForRemove;
										selectedForRemove ++;
										graph.erase(it);

										return info;
								}
								++selectedForRemove;
								if (selectedForRemove == graph.end()){
										selectedForRemove = graph.begin();
								}
						} while(!graph.empty() && selectedForRemove!= start);
				}
				return NULL;
		}*/
		/*
		InfoAdjacent *RemoveFirst(){
				if (graph.begin() == graph.end()){
						return NULL;
				}
				InfoAdjacent * ret = graph.begin()->second;
				graph.erase(graph.begin());
				ListOfRecordIds::iterator itList;
				for(itList = ret->list.begin(); itList != ret->list.end(); ++itList){
						InfoAdjacent *adjacent = graph[*itList];
						adjacent->list.remove(ret->id);
						adjacent->total --;
				}


				return ret;
		}*/
		InfoAdjacent *GetVertex(int id){
			PairGraph::iterator it = graph.find(id);
			if(it != graph.end()){
				return it->second.info;
			}
			return NULL;
		}
		bool HasVertex(int id){
			return graph.find(id) != graph.end();
		}
	  /**
	   * Remove the vertex with largest degree and its edges creating a subgraph
	   */
		InfoAdjacent * RemoveVertexWithLargestDegree(){
				assert(sorted);
				ListOfRecordIds::iterator itList;
				ListEntry entry;
				if (!listOfVertex.empty()){
					entry = listOfVertex.back();
					listOfVertex.pop_back();
					/*
					for(itList = entry.info->list->begin(); itList != entry.info->list->end(); ++itList){
						if (graph.find(*itList) != graph.end()){
							ListEntry e = graph[*itList];
							e.info->list->remove(entry.info->id);
							e.info->total --;
						}
					}
					*/
					graph.erase(entry.info->id);
					return entry.info;
				} else {
						return NULL;
				}
		}
		/*
		InfoAdjacent * RemoveVertexWithLargestDegree(){
		  assert(sorted);
		   ListEntry entry;
		  if (!listOfVertex.empty()){
			entry = listOfVertex.back();
			listOfVertex.pop_back();
			graph.erase(entry.info->id);
			return entry.info;
		  } else {
			return NULL;
		  }
		}*/
	private:	
	  PairGraph graph;
	  bool sorted;
    DegreeOrderedTAD listOfVertex;
		PairGraph::iterator selectedForRemove;
};
}
bool sortByDegree(ListEntry a, ListEntry b);
 
#endif

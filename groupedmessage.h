#ifndef GROUPEDMESSAGE_H_
#define GROUPEDMESSAGE_H_

#include "FilterDev.h"
#include "messagesfp.h"
#include "anthillutil.h"
#include <cassert>
#include <iostream>
using namespace std;

namespace Feraparda {
  template<class Message, class Group>
  class GroupedMessage{
	private:
	  /** Port used when messages require ack from receiver */
	  AnthillInputPort ackIn;
	  /** Port used to send messages when the buffer overflows */
	  AnthillOutputPort out;
	protected:
		AnthillUtil *ahUtil;
		int maxMessagesBeforeAck;
		int max;
		int totalOfSentMessages;
		Group *groups;
		int *counter;
		int *lastAckCounter;
		int lastGroup;
		int totalOfTargets;
		
		virtual bool Add(Message message) = 0;
		virtual void ConfigureNeedAck(Group group, bool needsAck) = 0;
		virtual void BeforeFlush(int target){}
		virtual Group GetGroup(){return groups[lastGroup];}
		void Recycle(int index){
			this->groups[index].total = 0;
			counter[index] = 0;
		}
	public:
		virtual ~GroupedMessage(){
			for(int i= 0; i < totalOfTargets; i++){
				if (groups[i].total > 0){
					cerr << "Group "<< i << " still has "  << groups[i].total << " messages to be sent\n";
				}
			}
			delete[] groups;
			delete[] counter;
			delete[] lastAckCounter;
		}
	  GroupedMessage(AnthillUtil *ahUtil,
			  AnthillOutputPort out, int totalOfTargets, int max){

			this->ahUtil = ahUtil;
			this->maxMessagesBeforeAck = 0;

			this->out = out;
			this->max = max;
			this->totalOfTargets = totalOfTargets;
			
			counter = new int[totalOfTargets];
			lastAckCounter = new int[totalOfTargets];
			//messages = new Message[max];
			assert(totalOfTargets > 0);
			groups = new Group[totalOfTargets];
			for(int i = 0; i < totalOfTargets; i ++){
			  counter[i] = 0; 
			  lastAckCounter[i] = 0;
			}
			lastGroup = 0;
			totalOfSentMessages = 0;
	  }
		void Stats(){
			for (int i = 0; i < totalOfTargets; i++) {
				cout << "Target " << i << " has " <<  counter[i] << " elements " << endl;
			}
		}
		virtual void Flush(){
			//Stats();
			for(int i = 0; i < totalOfTargets; i++){
				//cout << " @@@@ " << counter[i] << endl;
				Flush(i);
			}
		}
		virtual void Flush(int target){
			//cout << "Flushing " << target << " : " << counter[target] << endl;
			if (counter[target] > 0){
				BeforeFlush(target);
				//cout << "Flushing message " << endl;
				/*
				 * Test if an ack message is needed, before keeping sending other
				 * messages
				 */
				if (maxMessagesBeforeAck > 0) {
					lastAckCounter[target] += counter[target];
					bool needsAck = lastAckCounter[target] > maxMessagesBeforeAck;
					ConfigureNeedAck(groups[target], needsAck);

					ahUtil->Send(out, &groups[target], sizeof(Group));
					if (needsAck){
						lastAckCounter[target] = 0;
						int status = 0;
						ahUtil->Read(ackIn, &status, sizeof(int));
					}
				} else {

					ahUtil->Send(out, &groups[target], sizeof(Group));
				}
				Recycle(target);
			}
		}
		virtual bool Send(Message msg){
			totalOfSentMessages ++;
			if (Add(msg)){ //If it's full
				Flush(lastGroup);
				return true;
		 	}
			return false;
		}
	  bool IsFull(int index){return counter[index] == max; }
	  int getCounter(int index){return counter[index];}
	  AnthillOutputPort getOut(){return out;}
	  void ConfigureAck(int max, AnthillInputPort ackIn){
		  this->maxMessagesBeforeAck = max;
		  this->ackIn = ackIn;
	  }
	  int getTotalOfSentMessages(){return totalOfSentMessages;}
  };
}
#endif /*GROUPEDMESSAGE_H_*/

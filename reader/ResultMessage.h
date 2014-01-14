#ifndef RESULTMESSAGE_H_
#define RESULTMESSAGE_H_

/**
 * A grouped result message handler
 */
class ResultMessage: public GroupedMessage<result_msg_t, result_grouped_msg_t> {
protected:
	virtual void ConfigureNeedAck(result_grouped_msg_t group, bool needsAck){
		throw FerapardaException("Message does not support ACK.");
	}

	virtual bool Add(result_msg_t msg) {
		int target = msg.id2 % this->totalOfTargets;
		this->lastGroup = target;
		this->groups[lastGroup].msgs[counter[lastGroup]] = msg;
		this->counter[lastGroup]++;
		this->groups[lastGroup].total++;
		return (counter[lastGroup] >= this->max);
	}
public:
	ResultMessage(AnthillUtil *ahUtil, AnthillOutputPort out,
			int totalOfTargets, int max) :
		GroupedMessage<result_msg_t, result_grouped_msg_t> (ahUtil, out,
				totalOfTargets, max) {
		for (int i = 0; i < totalOfTargets; i++) {
			groups[i].mod = i;
			groups[i].total = 0;
		}
	}
};

#endif

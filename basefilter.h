/*
 * FerapardaBaseFilter.h
 *
 *  Created on: 12/08/2009
 *      Author: walter
 */

#ifndef FERAPARDABASEFILTER_H_
#define FERAPARDABASEFILTER_H_

#include <map>
#include <string>
#include <queue>
#include "anthillutil.h"
#include "logging.h"

/*Prototipos*/
extern "C" {
int initFilter(void* work, int size);
int processFilter(void* work, int size);
int finalizeFilter(void);
}
using namespace std;
using namespace Feraparda;

typedef queue<void*> * event_queue_t;
/**
 * Base class for filters in Feraparda.
 */
namespace Feraparda {
class FerapardaBaseFilter {
private:
protected:
	AnthillUtil *ahUtil;
	Logger * logger;
	map<string, event_queue_t> queues;
public:
	FerapardaBaseFilter() {
		ahUtil = NULL;
	}
	virtual ~FerapardaBaseFilter() {
		//FIXME: limpar queues
		map<string, event_queue_t>::iterator it;
		for(it = queues.begin(); it != queues.end(); it++){
			delete (*it).second;
		}
	}

	virtual void Process() = 0;
	void SetAnthillUtil(AnthillUtil *util) {
		this->ahUtil = util;
	}
	void ProcessEvent(AnthillInputPort port, void* data);
	void ReadAndEnqueueEvents(AnthillInputPort port, int size);
};
}
typedef struct {
	FerapardaBaseFilter *filter;
	AnthillInputPort *in;
	AnthillUtil *util;
	int size;
} receive_message_thread_param_t;
#endif /* FERAPARDABASEFILTER_H_ */

/*
 * FerapardaBaseFilter.cpp
 *
 *  Created on: 12/08/2009
 *      Author: walter
 */

#include "basefilter.h"
#include "ferapardaexception.h"
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <queue>
/**/
void *receive_message_thread_func(void *arg);


void FerapardaBaseFilter::ProcessEvent(AnthillInputPort port, void* data) {

}
void FerapardaBaseFilter::ReadAndEnqueueEvents(AnthillInputPort port, int size) {
	if (this->queues.find(port.GetName()) != this->queues.end()) {
		throw FerapardaException("Port is already been read.");
	} else {
		this->queues[port.GetName()] = new queue<void*>();

		pthread_t thread;
		receive_message_thread_param_t params;
		params.filter = this;
		params.in = &port;
		params.util = ahUtil;
		params.size = size;

		pthread_create(&(thread), NULL, receive_message_thread_func, &(params));
	}
}
void *receive_message_thread_func(void *arg) {
	receive_message_thread_param_t params = *(receive_message_thread_param_t *) arg;
	AnthillUtil *util = params.util;
	FerapardaBaseFilter *filter = params.filter;
	AnthillInputPort *in = params.in;

	char *buffer = new char[params.size];
	cout << "Iniciando monitoramento " << endl;
	while (util->Read(*in, buffer, params.size) != EOW) {
		cout << "Chegou mensagem " << endl;
	}
	cout << "Terminou monitoramento " << endl;

	delete[] buffer;
	return NULL;
}
//

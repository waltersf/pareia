#include "../messagesfp.h"
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
extern "C" {
	int hash(char *label, int image){
	    return atoi(label);
	}
	void getLabel(void *originalMsg, int size, char *label){
	    record_grouped_msg_t *msg = (record_grouped_msg_t *) originalMsg;
	    sprintf(label, "%d", msg->mod);
	}
}

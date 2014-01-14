#include "../messagesfp.h"
#include <cstdio>
#include <cstdlib>
extern "C" {
	int hash(char *label, int image){
	    return atoi(label);
	}
	void getLabel(void *originalMsg, int size, char *label){
	    block_grouped_msg_t* msg = (block_grouped_msg_t*) originalMsg;
	    sprintf(label, "%d", msg->mod);
	}
}	

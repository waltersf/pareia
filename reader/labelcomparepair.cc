#include "../messagesfp.h"
#include <cstdio>
#include <cstdlib>
using namespace std;
extern "C" {
	int hash(char *label, int image){
	    return atoi(label);
	}
	void getLabel(void *originalMsg, int size, char *label){
	    compare_pair_grouped_msg_t *msg = (compare_pair_grouped_msg_t*) originalMsg;
	    sprintf(label, "%d", msg->mod);
		//printf("@@@ Rota %d Seq %d\n", msg->mod, msg->seq);
	}
}

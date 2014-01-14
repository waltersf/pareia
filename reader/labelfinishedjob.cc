#include "../messagesfp.h"
#include <cstdlib>
int hash(char *label, int image){
    return atoi(label);
}

void getLabel(void *originalMsg, int size, char *label){
    //finished_job_msg* msg = (finished_job_msg*) originalMsg;
    //sprintf(label, "%d", msg->mod);
}


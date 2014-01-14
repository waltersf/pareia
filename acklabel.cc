#include <stdio.h>
#include <stdlib.h>
extern "C" {
	int hash(char *label, int image){
	    return atoi(label);
	}
	void getLabel(void *originalMsg, int size, char *label){
	    int *target = (int*) originalMsg;
	    sprintf(label, "%d", *target);
	}
}

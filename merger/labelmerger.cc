#include "../messagesfp.h"
#include <cstdio>
#include <ctime>
#include <cstdlib>
int hash(char *label, int image){
    return atoi(label);
}

/* funcão responsável pela identificação do filtro destino da mensagem */
void getLabel(void *originalMsg, int size, char *label){
 record_pair_grouped_msg_t *msg = (record_pair_grouped_msg_t *) originalMsg;
  sprintf(label, "%d", msg->mod);
}


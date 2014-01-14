#include "../messagesfp.h"
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
int hash(char *label, int image){
    return atoi(label);
}
/* funcão responsável pela identificação do filtro destino da mensagem */
void getLabel(void *originalMsg, int size, char *label){
    use_cache_grouped_msg_t *msg = (use_cache_grouped_msg_t*) originalMsg;
    sprintf(label, "%d", msg->mod);
}


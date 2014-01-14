#ifndef _MESSAGES_FP_H
#define _MESSAGES_FP_H
#include "constants.h"
/**
 * Struct used in blocking message.
 * The <code>flags</code> field contains informations about blocking, encoded as
 * a bitmask:
 * - Bit (from less significative - right to left):
 * 		0 - 4: Blocking conjunctions. So, it is possible up to 32 different blocking conjunctions.
 * 		5: Future expansion - not used
 * 		6: Data source that generated the blocking key. When the task is deduplication, only
 * 			one data source is used.
 * 		7: Type of task. 0 means deduplication; 1 record linkage
 * Use macros defined in this header file (BLOCKING_FLAG_* and IS_BLOCKING_FLAG_*) to set/get bits.
 */
typedef struct {
  int recnum;
  unsigned char flags;
  char key[MAX_BLOCK_KEY_SIZE];
} block_msg_t;

typedef struct {
  unsigned char requiresAck;
  int mod;
  int total;
  block_msg_t msgs[GROUP_SIZE_MSG_ADD_RECORD];
} block_grouped_msg_t;

typedef struct {
  unsigned int id1;
  unsigned int id2;
  unsigned int blockId;
} record_pair_msg_t;

typedef struct {
  record_pair_msg_t msgs[GROUP_SIZE_MSG_PAIR];
  int mod;
  int total;
} record_pair_grouped_msg_t;

typedef char * compare_pair_msg_t;
typedef struct {
  int mod;
  int total;
  int source;
  int seq;
  char data[COMPARE_PAIR_GROUPED_DATA_SIZE];
} compare_pair_grouped_msg_t;

typedef struct {
  unsigned int id1;
  unsigned int id2; //needed for labeling streaming
  unsigned int blockId;
  char record[MAX_RECORD_SIZE];
} record_msg_t;

typedef struct {
  unsigned int id1;
  unsigned int id2;
  unsigned int blockId;
} use_cache_msg_t;

typedef struct {
  unsigned int id1;
  unsigned int id2;
  unsigned int blockId;
  char key1[MAX_KEY_SIZE];
  char key2[MAX_KEY_SIZE];
  float result;
  float weightResult;
  char resultVector [200];
} result_msg_t;

typedef struct {
  result_msg_t msgs[GROUP_SIZE_MSG_RESULT];
  int total;
  char mod;
} result_grouped_msg_t;
#endif

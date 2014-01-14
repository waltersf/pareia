#ifndef _CONSTANTS_H
#define _CONSTANTS_H

/*
 * Maximum number of pairs kept in hashes to perform distintness.
 */
#define DISTINCT_QUEUE_SIZE 100000

#define MIN_BLOCK_KEY_SIZE 2

#define MAX_KEY_SIZE 20
#define MAX_BLOCK_KEY_SIZE 750
#define MAX_BLOCK_TYPES 60
#define MAX_RECORD_SIZE 650

#define GROUP_SIZE_MSG_ADD_RECORD 140//140
#define GROUP_SIZE_MSG_PAIR  400 //500

#define GROUP_SIZE_MSG_RECORD 20
#define GROUP_SIZE_MSG_REDO 511

#define GROUP_SIZE_MSG_RESULT  100 /*maximum number of pairs to send in a message to result filter*/


#define COMPARE_PAIR_GROUPED_DATA_SIZE 4080
#define MESSAGE_USE_CACHE 1
#define MESSAGE_RECORD 2

#define BLOCKING_FLAG_LINKAGE(flag) flag |= 0x80
#define BLOCKING_FLAG_DATASOURCE_1(flag) flag |= 0x40
#define BLOCKING_FLAG_BLOCKING_ID(flag, id) flag += (unsigned char) id

#define IS_BLOCKING_FLAG_LINKAGE(flag) ((flag & 0x80) == 0x80)
#define IS_BLOCKING_FLAG_DATA_SOURCE_1(flag) ((flag & 0x40) == 0x40)
#define GET_BLOCKING_ID(flag) (flag & 0x1F )

#endif

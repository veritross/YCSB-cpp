#ifndef KVS_CONST_H
#define KVS_CONST_H

#define KVS_MIN_KEY_LENGTH 4
#define KVS_MAX_KEY_LENGTH 255
#define KVS_MIN_VALUE_LENGTH 0
#define KVS_MAX_VALUE_LENGTH (2 * 1024 * 1024)
#define KVS_ALIGNMENT_UNIT 512            /*value of KVS_ALIGNMENT_UNIT must be a power of 2 currently */
#define KVS_VALUE_LENGTH_ALIGNMENT_UNIT 4 /*value of KV_VALUE_LENGTH_ALIGNMENT_UNIT must be a power of 2 currently */

#endif // KVS_CONST_H
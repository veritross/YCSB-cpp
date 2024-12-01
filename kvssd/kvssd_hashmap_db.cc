#include <string>
#include <mutex>
#include "kvssd_hashmap_db.h"

#include <kvssd.h>

const char *kvstrerror[] = {
    "Successful",                           // KVS_SUCCESS
    "Buffer space is not enough",           // KVS_ERR_BUFFER_SMALL
    "Device does not have enough space",    // KVS_ERR_DEV_CAPAPCITY
    "No device with the dev_hd exists",     // KVS_ERR_DEV_NOT_EXIST
    "Key space does not have enough space", // KVS_ERR_KS_CAPACITY
    "Key space is already created",         // KVS_ERR_KS_EXIST
    "Index is not valid",                   // KVS_ERR_KS_INDEX
    "Key space name is not valid",          // KVS_ERR_KS_NAME
    "Key space does not exist",             // KVS_ERR_KS_NOT_EXIST
    "Key space does not open",              // KVS_ERR_KS_NOT_OPEN
    "Key space is already opened",          // KVS_ERR_KS_OPEN
    "Iterator filter is not valid",         // KVS_ERR_ITERATOR_FILTER_INVALID
    "Maximum number of iterators opened",   // KVS_ERR_ITERATOR_MAX
    "Iterator Key Group does not exist",    // KVS_ERR_ITERATOR_NOT_EXIST
    "Iterator is already opened",           // KVS_ERR_ITERATOR_OPEN
    "Key is not valid",                     // KVS_ERR_KEY_LENGTH_INVALID
    "Key does not exist",                   // KVS_ERR_KEY_NOT_EXIST
    "Option is not supported",              // KVS_ERR_OPTION_INVALID
    "Null input parameter",                 // KVS_ERR_PARAM_INVALID
    "I/O error occurs",                     // KVS_ERR_SYS_IO
    "Value length is out of range",         // KVS_ERR_VALUE_LENGTH_INVALID
    "Value offset is out of range",         // KVS_ERR_VALUE_OFFSET_INVALID
    "Value offset is misaligned",           // KVS_ERR_VALUE_OFFSET_MISALIGNED
    "Value update is not allowed",          // KVS_ERR_VALUE_UPDATE_NOT_ALLOWED
    "Device was not opened yet"             // KVS_ERR_DEV_NOT_OPENED
};

Hashmap_KVSSD::Hashmap_KVSSD()
{
    // initialize
}
Hashmap_KVSSD::~Hashmap_KVSSD()
{
    // cleanup
}

/** @todo
 * @brief Read API 함수
 * @param[in] key 찾을 row의 key
 * @param[out] value_out 찾은 row의 value
 * @return kvs_result
 */
kvs_result Hashmap_KVSSD::Read(const kvs_key &key, kvs_value &value_out)
{
    return KVS_SUCCESS;
}

/** @todo
 * @brief Insert API 함수
 * @param[in] key 추가할 row의 key
 * @param[in] value 추가할 row의 value
 * @return kvs_result
 */
kvs_result Hashmap_KVSSD::Insert(const kvs_key &key, const kvs_value &value)
{
    return KVS_SUCCESS;
}

/** @todo
 * @brief Update API 함수
 * @param[in] key 검색 및 변경할 row의 key
 * @param[in] value 검색 및 변경할 row의 value
 * @return kvs_result
 */
kvs_result Hashmap_KVSSD::Update(const kvs_key &key, const kvs_value &value)
{
    return KVS_SUCCESS;
}

/** @todo
 * @brief Delete API 함수
 * @param[in] key 삭제할 row의 key
 * @return kvs_result
 */
kvs_result Hashmap_KVSSD::Delete(const kvs_key &key)
{
    return KVS_SUCCESS;
}

KVSSD *NewKvssdDB()
{
    return new Hashmap_KVSSD;
}
#include "kvssd_hashmap_db.h"

#include <string>

namespace kvssd_hashmap
{

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
        pthread_rwlock_init(&rwl, NULL);
    }
    Hashmap_KVSSD::~Hashmap_KVSSD()
    {
        pthread_rwlock_destroy(&rwl);
    }

    kvssd::kvs_result Hashmap_KVSSD::ValidateRequest(const kvssd::kvs_key &key, std::optional<std::reference_wrapper<const kvssd::kvs_value>> value)
    {
        if (key.length < KVS_MIN_KEY_LENGTH || KVS_MAX_KEY_LENGTH < key.length)
        {
            return kvssd::kvs_result::KVS_ERR_KEY_LENGTH_INVALID;
        }
        if (key.key == NULL)
        {
            return kvssd::kvs_result::KVS_ERR_PARAM_INVALID;
        }
        if (value)
        {
            if (value->get().length < KVS_MIN_VALUE_LENGTH || KVS_MAX_VALUE_LENGTH < value->get().length)
            {
                return kvssd::kvs_result::KVS_ERR_KEY_LENGTH_INVALID;
            }
            if (value->get().offset & (KVS_ALIGNMENT_UNIT - 1))
            {
                return kvssd::kvs_result::KVS_ERR_VALUE_OFFSET_MISALIGNED;
            }
            if (value->get().value == NULL && value->get().length)
            {
                return kvssd::kvs_result::KVS_ERR_PARAM_INVALID;
            }
        }
        return kvssd::kvs_result::KVS_SUCCESS;
    }

    // API 함수 4가지
    kvssd::kvs_result Hashmap_KVSSD::Read(const kvssd::kvs_key &key, kvssd::kvs_value &value_out)
    {
        kvssd::kvs_result ret = ValidateRequest(key, value_out);
        if (ret != kvssd::kvs_result::KVS_SUCCESS)
        {
            return ret;
        }
        pthread_rwlock_rdlock(&rwl);
        auto it = db.find(key);
        pthread_rwlock_unlock(&rwl);
        if (it == db.end())
        {
            return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
        }
        value_out = it->second;
        return kvssd::kvs_result::KVS_SUCCESS;
    }

    kvssd::kvs_result Hashmap_KVSSD::Insert(const kvssd::kvs_key &key, const kvssd::kvs_value &value)
    {
        kvssd::kvs_result ret = ValidateRequest(key, value);
        if (ret != kvssd::kvs_result::KVS_SUCCESS)
        {
            return ret;
        }
        pthread_rwlock_wrlock(&rwl);
        auto it = db.find(key);
        if (it != db.end())
        {
            pthread_rwlock_unlock(&rwl);
            return kvssd::kvs_result::KVS_ERR_KS_EXIST;
        }
        db.insert({key, value});
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_SUCCESS;
    }

    kvssd::kvs_result Hashmap_KVSSD::Update(const kvssd::kvs_key &key, const kvssd::kvs_value &value)
    {
        kvssd::kvs_result ret = ValidateRequest(key, value);
        if (ret != kvssd::kvs_result::KVS_SUCCESS)
        {
            return ret;
        }
        pthread_rwlock_wrlock(&rwl);
        auto it = db.find(key);
        if (it == db.end())
        {
            pthread_rwlock_unlock(&rwl);
            return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
        }
        it->second = value;
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_SUCCESS;
    }

    kvssd::kvs_result Hashmap_KVSSD::Delete(const kvssd::kvs_key &key)
    {
        kvssd::kvs_result ret = ValidateRequest(key, std::nullopt);
        if (ret != kvssd::kvs_result::KVS_SUCCESS)
        {
            return ret;
        }
        pthread_rwlock_wrlock(&rwl);
        auto it = db.find(key);
        if (it == db.end())
        {
            pthread_rwlock_unlock(&rwl);
            return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
        }
        db.erase(key);
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_SUCCESS;
    }

} // namespace kvssd_hashmap
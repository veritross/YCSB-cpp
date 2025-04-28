#ifndef YCSB_C_KVSSD_H_
#define YCSB_C_KVSSD_H_

#include <pthread.h>

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <optional>
#include <unordered_map>

#include "core/db_factory.h"
#include "kvssd_const.h"

namespace kvssd {

enum class kvs_result {
    KVS_SUCCESS = 0,                // Successful
    KVS_ERR_BUFFER_SMALL = 0x001,   // buffer space is not enough
    KVS_ERR_DEV_CAPAPCITY = 0x002,  // device does not have enough space. Key Space size is too big
    KVS_ERR_DEV_NOT_EXIST = 0x003,  // no device with the dev_hd exists
    KVS_ERR_KS_CAPACITY = 0x004,    // key space does not have enough space
    KVS_ERR_KS_EXIST = 0x005,       // key space is already created with the same name
    KVS_ERR_KS_INDEX = 0x006,       // index is not valid
    KVS_ERR_KS_NAME = 0x007,        // key space name is not valid
    KVS_ERR_KS_NOT_EXIST = 0x008,   // key space does not exist
    KVS_ERR_KS_NOT_OPEN = 0x009,    // key space does not open
    KVS_ERR_KS_OPEN = 0x00A,        // key space is already opened
    KVS_ERR_ITERATOR_FILTER_INVALID =
        0x00B,                           // iterator filter(match bitmask and pattern) is not valid
    KVS_ERR_ITERATOR_MAX = 0x00C,        // the maximum number of iterators that a
                                         // device supports is opened
    KVS_ERR_ITERATOR_NOT_EXIST = 0x00D,  // the iterator Key Group does not exist
    KVS_ERR_ITERATOR_OPEN = 0x00E,       // iterator is already opened
    KVS_ERR_KEY_LENGTH_INVALID = 0x00F,  // key is not valid (e.g., key length is not supported)
    KVS_ERR_KEY_NOT_EXIST = 0x010,       // key does not exist
    KVS_ERR_OPTION_INVALID = 0x011,      // an option is not supported in this implementation
    KVS_ERR_PARAM_INVALID = 0x012,       // null input parameter
    KVS_ERR_SYS_IO = 0x013,              // I/O error occurs
    KVS_ERR_VALUE_LENGTH_INVALID = 0x014,  // value length is out of range
    KVS_ERR_VALUE_OFFSET_INVALID = 0x015,  // value offset is out of range
    KVS_ERR_VALUE_OFFSET_MISALIGNED =
        0x016,  // offset of value is required to be aligned to KVS_ALIGNMENT_UNIT
    KVS_ERR_VALUE_UPDATE_NOT_ALLOWED = 0x017,  // key exists but value update is not allowed
    KVS_ERR_DEV_NOT_OPENED = 0x018,            // device was not opened yet
};

constexpr std::array<std::string_view, 26> kvstrerror{
    // kvs_result as index, Error statement as value
    {
        "Successful",                            // KVS_SUCCESS
        "Buffer space is not enough",            // KVS_ERR_BUFFER_SMALL
        "Device does not have enough space",     // KVS_ERR_DEV_CAPAPCITY
        "No device with the dev_hd exists",      // KVS_ERR_DEV_NOT_EXIST
        "Key space does not have enough space",  // KVS_ERR_KS_CAPACITY
        "Key space is already created",          // KVS_ERR_KS_EXIST
        "Index is not valid",                    // KVS_ERR_KS_INDEX
        "Key space name is not valid",           // KVS_ERR_KS_NAME
        "Key space does not exist",              // KVS_ERR_KS_NOT_EXIST
        "Key space does not open",               // KVS_ERR_KS_NOT_OPEN
        "Key space is already opened",           // KVS_ERR_KS_OPEN
        "Iterator filter is not valid",          // KVS_ERR_ITERATOR_FILTER_INVALID
        "Maximum number of iterators opened",    // KVS_ERR_ITERATOR_MAX
        "Iterator Key Group does not exist",     // KVS_ERR_ITERATOR_NOT_EXIST
        "Iterator is already opened",            // KVS_ERR_ITERATOR_OPEN
        "Key is not valid",                      // KVS_ERR_KEY_LENGTH_INVALID
        "Key does not exist",                    // KVS_ERR_KEY_NOT_EXIST
        "Option is not supported",               // KVS_ERR_OPTION_INVALID
        "Null input parameter",                  // KVS_ERR_PARAM_INVALID
        "I/O error occurs",                      // KVS_ERR_SYS_IO
        "Value length is out of range",          // KVS_ERR_VALUE_LENGTH_INVALID
        "Value offset is out of range",          // KVS_ERR_VALUE_OFFSET_INVALID
        "Value offset is misaligned",            // KVS_ERR_VALUE_OFFSET_MISALIGNED
        "Value update is not allowed",           // KVS_ERR_VALUE_UPDATE_NOT_ALLOWED
        "Device was not opened yet"              // KVS_ERR_DEV_NOT_OPENED
    }};

struct kvs_key {
    void *key;
    uint16_t length;

    friend bool operator==(const kvs_key &lhs, const kvs_key &rhs) {
        return lhs.length == rhs.length && std::memcmp(lhs.key, rhs.key, lhs.length) == 0;
    }
};

struct kvs_value {
    void *value;                 // value byte stream buffer's start address
    uint32_t length;             // value byte stream buffer size (byte)
    uint32_t actual_value_size;  // device stored value's actual size (byte)
    uint32_t offset;             // [optional] device stored value offset (byte)
};

class KVSSD {
   public:
    KVSSD() = default;
    virtual ~KVSSD() = default;

    virtual kvs_result Read(const kvs_key &, kvs_value &) = 0;
    virtual kvs_result Insert(const kvs_key &, const kvs_value &) = 0;
    virtual kvs_result Update(const kvs_key &, const kvs_value &) = 0;
    virtual kvs_result Delete(const kvs_key &) = 0;
};

}  // namespace kvssd

ycsbc::DB *NewKvssdDB();

#endif  // YCSB_C_KVSSD_H_
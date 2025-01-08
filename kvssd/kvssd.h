#ifndef YCSB_C_KVSSD_H_
#define YCSB_C_KVSSD_H_

#include "kvssd_const.h"
#include <cstring>
#include <cstdint>
#include <optional>
#include <pthread.h>
#include <unordered_map>
#include <condition_variable>

namespace kvssd
{

    enum class kvs_result
    {
        KVS_SUCCESS = 0,                          // Successful
        KVS_ERR_BUFFER_SMALL = 0x001,             // buffer space is not enough
        KVS_ERR_DEV_CAPAPCITY = 0x002,            // device does not have enough space. Key Space size is too big
        KVS_ERR_DEV_NOT_EXIST = 0x003,            // no device with the dev_hd exists
        KVS_ERR_KS_CAPACITY = 0x004,              // key space does not have enough space
        KVS_ERR_KS_EXIST = 0x005,                 // key space is already created with the same name
        KVS_ERR_KS_INDEX = 0x006,                 // index is not valid
        KVS_ERR_KS_NAME = 0x007,                  // key space name is not valid
        KVS_ERR_KS_NOT_EXIST = 0x008,             // key space does not exist
        KVS_ERR_KS_NOT_OPEN = 0x009,              // key space does not open
        KVS_ERR_KS_OPEN = 0x00A,                  // key space is already opened
        KVS_ERR_ITERATOR_FILTER_INVALID = 0x00B,  // iterator filter(match bitmask and pattern) is not valid
        KVS_ERR_ITERATOR_MAX = 0x00C,             // the maximum number of iterators that a device supports is opened
        KVS_ERR_ITERATOR_NOT_EXIST = 0x00D,       // the iterator Key Group does not exist
        KVS_ERR_ITERATOR_OPEN = 0x00E,            // iterator is already opened
        KVS_ERR_KEY_LENGTH_INVALID = 0x00F,       // key is not valid (e.g., key length is not supported)
        KVS_ERR_KEY_NOT_EXIST = 0x010,            // key does not exist
        KVS_ERR_OPTION_INVALID = 0x011,           // an option is not supported in this implementation
        KVS_ERR_PARAM_INVALID = 0x012,            // null input parameter
        KVS_ERR_SYS_IO = 0x013,                   // I/O error occurs
        KVS_ERR_VALUE_LENGTH_INVALID = 0x014,     // value length is out of range
        KVS_ERR_VALUE_OFFSET_INVALID = 0x015,     // value offset is out of range
        KVS_ERR_VALUE_OFFSET_MISALIGNED = 0x016,  // offset of value is required to be aligned to KVS_ALIGNMENT_UNIT
        KVS_ERR_VALUE_UPDATE_NOT_ALLOWED = 0x017, // key exists but value update is not allowed
        KVS_ERR_DEV_NOT_OPENED = 0x018,           // device was not opened yet
    };

    extern const char *kvstrerror[]; // kvs_result을 Index, 대응되는 에러문을 Value로 갖는 배열

    struct kvs_key
    {
        void *key;
        uint16_t length;

        bool operator==(const kvs_key &other) const
        {
            return length == other.length &&
                   std::memcmp(key, other.key, length) == 0;
        }
    };

    struct kvs_value
    {
        void *value;                // value byte stream 버퍼의 시작 주소
        uint32_t length;            // value byte stream 버퍼의 크기 (단위: byte)
        uint32_t actual_value_size; // device에 저장된 value의 실제 크기 (단위: byte)
        uint32_t offset;            // [optional] device에 저장된 value의 offset (단위: byte)
    };

    class KVSSD : public ycsbc::DB
    {
    public:
        KVSSD() = default;
        virtual ~KVSSD() = default;

        virtual kvs_result Read(const kvs_key &, kvs_value &) = 0;
        virtual kvs_result Insert(const kvs_key &, const kvs_value &) = 0;
        virtual kvs_result Update(const kvs_key &, const kvs_value &) = 0;
        virtual kvs_result Delete(const kvs_key &) = 0;
    };

} // namespace kvssd

#endif // YCSB_C_KVSSD_H_
#ifndef YCSB_C_KVSSD_HASHMAP_DB_H_
#define YCSB_C_KVSSD_HASHMAP_DB_H_

#include <pthread.h>

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <optional>
#include <unordered_map>

#include "kvssd.h"
#include "kvssd_const.h"

namespace std {
template <>
struct hash<kvssd::kvs_key> {
    size_t operator()(const kvssd::kvs_key &k) const {
        const auto *data = static_cast<const uint8_t *>(k.key);
        uint16_t length = k.length;

        size_t hash = 0xcbf29ce484222325;  // FNV-1a init value (64 bits)
        const size_t fnv_prime = 0x100000001b3;

        for (uint16_t i = 0; i < length; ++i) {
            hash ^= data[i];    // Byte XOR
            hash *= fnv_prime;  // FNV-1a Hash Algorithm
        }

        return hash;
    }
};
}  // namespace std

namespace kvssd_hashmap {
class Hashmap_KVSSD : public kvssd::KVSSD {
   public:
    Hashmap_KVSSD();
    ~Hashmap_KVSSD() final;

    kvssd::kvs_result Read(const kvssd::kvs_key &, kvssd::kvs_value &) final;
    kvssd::kvs_result Insert(const kvssd::kvs_key &, const kvssd::kvs_value &) final;
    kvssd::kvs_result Update(const kvssd::kvs_key &, const kvssd::kvs_value &) final;
    kvssd::kvs_result Delete(const kvssd::kvs_key &) final;

   private:
    std::unordered_map<kvssd::kvs_key, kvssd::kvs_value> db;
    pthread_rwlock_t rwl;

    kvssd::kvs_result ValidateRequest(
        const kvssd::kvs_key &,
        std::optional<std::reference_wrapper<const kvssd::kvs_value>>) const;
    kvssd::kvs_key DeepCopyKey(const kvssd::kvs_key &) const;
    kvssd::kvs_value DeepCopyValue(const kvssd::kvs_value &) const;
};

}  // namespace kvssd_hashmap

#endif  // YCSB_C_KVSSD_HASHMAP_DB_H_
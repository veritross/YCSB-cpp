#ifndef YCSB_C_KVSSD_HASHMAP_DB_H_
#define YCSB_C_KVSSD_HASHMAP_DB_H_

#include "kvssd.h"
#include "kvssd_const.h"
#include <cstring>
#include <cstdint>
#include <optional>
#include <pthread.h>
#include <unordered_map>
#include <condition_variable>

namespace std
{
    template <>
    struct hash<kvssd::kvs_key>
    {
        size_t operator()(const kvssd::kvs_key &k) const
        {
            const uint8_t *data = static_cast<const uint8_t *>(k.key); // 바이트 단위로 접근
            uint16_t length = k.length;

            size_t hash = 0xcbf29ce484222325; // FNV-1a 초기값 (64비트)
            const size_t fnv_prime = 0x100000001b3;

            for (uint16_t i = 0; i < length; ++i)
            {
                hash ^= data[i];   // 바이트를 XOR 연산
                hash *= fnv_prime; // FNV-1a 해시 알고리즘의 주요 곱셈 단계
            }

            return hash;
        }
    };
} // namespace std

namespace kvssd_hashmap
{
    class Hashmap_KVSSD : public kvssd::KVSSD
    {
    public:
        Hashmap_KVSSD();
        ~Hashmap_KVSSD();

        kvssd::kvs_result Read(const kvssd::kvs_key &, kvssd::kvs_value &);
        kvssd::kvs_result Insert(const kvssd::kvs_key &, const kvssd::kvs_value &);
        kvssd::kvs_result Update(const kvssd::kvs_key &, const kvssd::kvs_value &);
        kvssd::kvs_result Delete(const kvssd::kvs_key &);

        ycsbc::DB::Status Read(const std::string &table, const std::string &key,
                               const std::vector<std::string> *fields, std::vector<ycsbc::DB::Field> &result);
        ycsbc::DB::Status Scan(const std::string &table, const std::string &key, int len,
                               const std::vector<std::string> *fields, std::vector<std::vector<ycsbc::DB::Field>> &result);
        ycsbc::DB::Status Update(const std::string &table, const std::string &key, std::vector<ycsbc::DB::Field> &values);
        ycsbc::DB::Status Insert(const std::string &table, const std::string &key, std::vector<ycsbc::DB::Field> &values);
        ycsbc::DB::Status Delete(const std::string &table, const std::string &key);

    private:
        std::unordered_map<kvssd::kvs_key, kvssd::kvs_value> db;
        pthread_rwlock_t rwl;

        kvssd::kvs_result ValidateRequest(const kvssd::kvs_key &, std::optional<std::reference_wrapper<const kvssd::kvs_value>>);
    };

} // namespace kvssd_hashmap

#endif // YCSB_C_KVSSD_HASHMAP_DB_H_
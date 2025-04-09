#ifndef YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_
#define YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_

#include <atomic>
#include <memory>

#include "core/db.h"
#include "kvssd_hashmap_db.h"
#include "utils/utils.h"

namespace kvssd_hashmap {

struct kvs_row {
    kvssd::kvs_key *key;
    kvssd::kvs_value *value;
};

struct KvsRowDeleter {
    void operator()(kvs_row *row) const {
        if (row) {
            if (row->key) {
                if (row->key->key) {
                    free(row->key->key);
                }
                delete row->key;
            }
            if (row->value) {
                if (row->value->value) {
                    free(row->value->value);
                }
                delete row->value;
            }
            delete row;
        }
    }
};

void SerializeRow(const std::vector<ycsbc::DB::Field> &values, std::string *data);
void DeserializeRow(std::vector<ycsbc::DB::Field> *values, const char *data_ptr, size_t data_len);

std::unique_ptr<kvs_row, KvsRowDeleter> CreateRow(const std::string &key_in,
                                                  const std::vector<ycsbc::DB::Field> &value_in,
                                                  bool allocate_value);

void PrintRow(const kvssd::kvs_value &value);
void PrintFieldVector(const std::vector<ycsbc::DB::Field> &value);

void CheckAPI(const kvssd::kvs_result ret);

// Wrapper 함수 4가지
void ReadRow(const std::unique_ptr<kvssd::KVSSD> &kvssd, const std::string &key,
             std::vector<ycsbc::DB::Field> &value);
void InsertRow(const std::unique_ptr<kvssd::KVSSD> &kvssd, const std::string &key,
               const std::vector<ycsbc::DB::Field> &value);
void UpdateRow(const std::unique_ptr<kvssd::KVSSD> &kvssd, const std::string &key,
               const std::vector<ycsbc::DB::Field> &value);
void DeleteRow(const std::unique_ptr<kvssd::KVSSD> &kvssd, const std::string &key);

}  // namespace kvssd_hashmap

#endif  // YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_
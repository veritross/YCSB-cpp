#include "kvssd_hashmap_db_impl.h"

namespace kvssd_hashmap
{

    // Field vector to string pointer
    void SerializeRow(const std::vector<ycsbc::DB::Field> &values, std::string *data)
    {
        for (const ycsbc::DB::Field &field : values)
        {
            uint32_t len = field.name.size();
            data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
            data->append(field.name.data(), field.name.size());
            len = field.value.size();
            data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
            data->append(field.value.data(), field.value.size());
        }
    }

    // char* to Field vector pointer
    void DeserializeRow(std::vector<ycsbc::DB::Field> *values, const char *data_ptr, size_t data_len)
    {
        const char *p = data_ptr;
        const char *lim = p + data_len;
        while (p != lim)
        {
            assert(p < lim);
            uint32_t len = *reinterpret_cast<const uint32_t *>(p);
            p += sizeof(uint32_t);
            std::string field(p, static_cast<const size_t>(len));
            p += len;
            len = *reinterpret_cast<const uint32_t *>(p);
            p += sizeof(uint32_t);
            std::string value(p, static_cast<const size_t>(len));
            p += len;
            values->push_back({field, value});
        }
    }

    std::unique_ptr<kvs_row> CreateRow(const std::string &key_in, const std::vector<ycsbc::DB::Field> &value_in)
    {
        std::unique_ptr<char[]> key_data = std::make_unique<char[]>(key_in.size());
        std::memcpy(key_data.get(), key_in.data(), key_in.size());
        void *key = static_cast<void *>(key_data.get());
        uint16_t key_length = key_in.size();

        std::string value_sz;
        SerializeRow(value_in, &value_sz);

        std::unique_ptr<char[]> value_data = std::make_unique<char[]>(value_sz.size());
        std::memcpy(value_data.get(), value_sz.data(), value_sz.size());

        void *value = static_cast<void *>(value_data.get());
        uint32_t value_length = value_sz.size();
        uint32_t actual_value_size = value_sz.size();
        uint32_t offset = 0;

        std::unique_ptr<kvs_key> newKey = std::make_unique<kvs_key>();
        newKey->key = key;
        newKey->length = key_length;

        std::unique_ptr<kvs_value> newValue = std::make_unique<kvs_value>();
        newValue->value = static_cast<void *>(value_data.get());
        newValue->length = value_sz.size();
        newValue->actual_value_size = value_sz.size();
        newValue->offset = 0;

        std::unique_ptr<kvs_row> newRow = std::make_unique<kvs_row>();
        newRow->key = std::move(newKey);
        newRow->value = std::move(newValue);

        ManagedMemory::Add(std::move(key_data));
        ManagedMemory::Add(std::move(value_data));

        return newRow;
    }

    // kvs_value의 값을 출력하는 함수
    void PrintRow(const kvs_value &value)
    {
        std::vector<ycsbc::DB::Field> value_vec;
        DeserializeRow(&value_vec, static_cast<char *>(value.value), value.length);
        if (value_vec.size() == 0)
        {
            printf("The value has empty field.\n");
            return;
        }
        printf("Name: Value\n");
        for (auto const field : value_vec)
        {
            printf("%s: %s\n", field.name.data(), field.value.data());
        }
    }

    // Field vector의 값을 출력하는 함수
    void PrintFieldVector(const std::vector<ycsbc::DB::Field> &value)
    {
        printf("\n");
        if (value.size() == 0)
        {
            printf("The value has empty field.\n");
            return;
        }
        printf("Name: Value\n");
        for (auto const field : value)
        {
            printf("%s: %s\n", field.name.data(), field.value.data());
        }
        printf("\n");
    }

    void CheckAPI(kvs_result ret)
    {
        if (ret != kvs_result::KVS_SUCCESS)
        {
            throw ycsbc::utils::Exception(std::string(kvstrerror[static_cast<int>(ret)]));
        }
    }

    // Wrapper 함수 4가지
    void ReadRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, std::vector<ycsbc::DB::Field> &value)
    {
        value = {};
        std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
        CheckAPI(kvssd->Read(*newRow->key, *newRow->value));
        DeserializeRow(&value, static_cast<char *>(newRow->value->value), newRow->value->length);
    }

    void InsertRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<ycsbc::DB::Field> &value)
    {
        std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
        CheckAPI(kvssd->Insert(*newRow->key, *newRow->value));
    }

    void UpdateRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<ycsbc::DB::Field> &value)
    {
        std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
        CheckAPI(kvssd->Update(*newRow->key, *newRow->value));
    }

    void DeleteRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key)
    {
        std::unique_ptr<kvs_row> newRow = CreateRow(key, {});
        CheckAPI(kvssd->Delete(*newRow->key));
    }

} // namespace ycsbc
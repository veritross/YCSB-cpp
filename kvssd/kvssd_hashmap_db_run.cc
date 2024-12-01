#include "bits/stdc++.h"
#include "kvssd_hashmap_db.h"

/**
 * @brief key, value struct의 unique_ptr를 원소로 하는 구조체
 */
struct kvs_row
{
    std::unique_ptr<kvs_key> key;
    std::unique_ptr<kvs_value> value;
};

/**
 * @brief Field vector to string pointer
 * @param[in] values string으로 변환할 vector<Field>
 * @param[out] data 변환된 값을 저장할 string 포인터
 */
void SerializeRow(const std::vector<Field> &values, std::string *data)
{
    for (const Field &field : values)
    {
        uint32_t len = field.name.size();
        data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data->append(field.name.data(), field.name.size());
        len = field.value.size();
        data->append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
        data->append(field.value.data(), field.value.size());
    }
}

/**
 * @brief char* to Field vector pointer
 * @param[out] values 변환된 값을 저장할 vector<Field> pointer
 * @param[in] data_ptr vector<Field>으로 변환할 char*의 위치
 * @param[in] data_len 변환할 char*의 길이
 */
void DeserializeRow(std::vector<Field> *values, const char *data_ptr, size_t data_len)
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
    // assert(values->size() == field_count_);
}

/**
 * @brief 새 row를 생성하는 함수
 * @param[in] key_in 새 key를 담은 문자열
 * @param[in] value_in 새 value를 담은 vector<Field>
 * @return 새 row의 unique pointer
 */
std::unique_ptr<kvs_row> CreateRow(const std::string &key_in, const std::vector<Field> &value_in)
{
    std::unique_ptr<kvs_row> newRow = std::make_unique<kvs_row>();

    newRow->key = std::make_unique<kvs_key>();
    char *key_data = new char[key_in.size()];
    std::memcpy(key_data, key_in.data(), key_in.size());
    newRow->key->key = static_cast<void *>(key_data);
    newRow->key->length = key_in.size();

    newRow->value = std::make_unique<kvs_value>();
    std::string value_sz;
    SerializeRow(value_in, &value_sz);
    char *value_data = new char[value_sz.size()];
    std::memcpy(value_data, value_sz.data(), value_sz.size());
    newRow->value->value = static_cast<void *>(value_data);
    newRow->value->length = value_sz.size();
    newRow->value->actual_value_size = value_sz.size();
    newRow->value->offset = 0;

    return newRow;
}

class Exception : public std::exception
{
public:
    Exception(const std::string &message) : message_(message) {}
    const char *what() const noexcept
    {
        return message_.c_str();
    }

private:
    std::string message_;
};

/**
 * @brief kvssd API 함수의 반환값을 보고 에러 여부를 확인하는 함수
 *
 * 에러가 발생한 경우 에러 문구를 출력한 뒤 프로그램을 종료한다.
 * @param[in] ret kvssd API 함수의 반환값
 */
void CheckAPI(kvs_result ret)
{
    if (ret != KVS_SUCCESS)
    {
        // printf("kvssd error (errmsg: %s)\n", kvstrerror[ret]);
        throw Exception(std::string(kvstrerror[ret]));
    }
    printf("%s\n", kvstrerror[ret]);
}

int main(void)
{
    printf("[Debug] Test started\n");

    std::unique_ptr<KVSSD> kvssd(NewKvssdDB());

    std::string key1 = "key1";
    std::string key2 = "key2";
    std::vector<Field> value1 = {{"field1", "value1_1"}, {"field2", "value1_2"}};
    std::vector<Field> value2 = {{"field1", "value2_1"}, {"field2", "value2_2"}};
    std::unique_ptr<kvs_row> row1 = CreateRow(key1, value1);
    std::unique_ptr<kvs_row> row2 = CreateRow(key2, value2);

    CheckAPI(kvssd->Insert(*row1->key, *row1->value));
    CheckAPI(kvssd->Read(*row1->key, *row1->value));
    CheckAPI(kvssd->Update(*row1->key, *row1->value));
    CheckAPI(kvssd->Delete(*row1->key));

    printf("[Debug] Test ended\n");
    return 0;
}
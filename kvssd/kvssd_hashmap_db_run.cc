#include "bits/stdc++.h"
#include "kvssd_hashmap_db.h"

#define debug(x) printf("%s\n", (x))

/**
 * @brief CreateRow()를 통해 할당된 스마트 포인터를 관리하는 Class
 * 프로그램이 종료될 때 관리 중인 모든 스마트 포인터 Free
 */
class ManagedMemory
{
public:
    // 관리 대상 스마트 포인터 추가
    static void Add(std::unique_ptr<char[]> memory)
    {
        GetInstance().memoryList.push_back(std::move(memory));
    }

    // 소멸 시 모든 스마트 포인터 해제
    ~ManagedMemory()
    {
        memoryList.clear();
    }

private:
    std::vector<std::unique_ptr<char[]>> memoryList;

    // instance 변수는 함수 최초 실행 시 한 번만 생성됨 (static, singletone)
    static ManagedMemory &GetInstance()
    {
        static ManagedMemory instance;
        return instance;
    }

    // 복사, 대입 연산자 삭제
    ManagedMemory() = default;
    ManagedMemory(const ManagedMemory &) = delete;
    ManagedMemory &operator=(const ManagedMemory &) = delete;
};

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

    // char *value_data = new char[value_sz.size()];
    auto value_data = std::make_unique<char[]>(value_sz.size());
    std::memcpy(value_data.get(), value_sz.data(), value_sz.size());
    newRow->value->value = static_cast<void *>(value_data.get());
    newRow->value->length = value_sz.size();
    newRow->value->actual_value_size = value_sz.size();
    newRow->value->offset = 0;

    ManagedMemory::Add(std::move(value_data));

    return newRow;
}

/**
 * @brief kvs_value의 값을 출력하는 함수
 * @param[in] value 출력할 kvs_value
 */
void PrintRow(const kvs_value &value)
{
    std::vector<Field> value_vec;
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

/**
 * @brief Field vector의 값을 출력하는 함수
 * @param[in] value 출력할 std::vector<Field>
 */
void PrintFieldVector(const std::vector<Field> &value)
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
    printf("...%s\n", kvstrerror[ret]);
}

/**
 * @brief Read Wrapper 함수
 *
 * @param[in] kvssd kvssd db 인스턴스
 * @param[in] key 검색할 key 값(string)
 * @param[out] value 찾았을 시 반환받을 value 값(std::vector<Field>)
 */
void ReadRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, std::vector<Field> &value)
{
    printf("Run Read...\n");
    value = {};
    std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
    CheckAPI(kvssd->Read(*newRow->key, *newRow->value));
    DeserializeRow(&value, static_cast<char *>(newRow->value->value), newRow->value->length);
}

/**
 * @brief Insert Wrapper 함수
 *
 * @param[in] kvssd kvssd db 인스턴스
 * @param[in] key 추가할 key 값(string)
 * @param[in] value 추가할 value 값(std::vector<Field>)
 */
void InsertRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<Field> &value)
{
    printf("Run Insert...\n");
    std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
    CheckAPI(kvssd->Insert(*newRow->key, *newRow->value));
}

/**
 * @brief Update Wrapper 함수
 *
 * @param[in] kvssd kvssd db 인스턴스
 * @param[in] key 찾을 key 값(string)
 * @param[in] value 변경할 value 값(std::vector<Field>)
 */
void UpdateRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<Field> &value)
{
    printf("Run Update...\n");
    std::unique_ptr<kvs_row> newRow = CreateRow(key, value);
    CheckAPI(kvssd->Update(*newRow->key, *newRow->value));
}

/**
 * @brief Delete Wrapper 함수
 *
 * @param[in] kvssd kvssd db 인스턴스
 * @param[in] key 찾을 key 값(string)
 */
void DeleteRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key)
{
    printf("Run Delete...\n");
    std::unique_ptr<kvs_row> newRow = CreateRow(key, {});
    CheckAPI(kvssd->Delete(*newRow->key));
}

int main(void)
{
    printf("[Debug] Test started\n");

    std::unique_ptr<KVSSD> kvssd(NewKvssdDB());

    std::string key1 = "key1";
    std::string key2 = "key2";
    std::vector<Field> value1 = {{"field1", "value1_1"}, {"field2", "value1_2"}};
    std::vector<Field> value2 = {{"field1", "value2_1"}, {"field2", "value2_2"}};
    std::vector<Field> output_value;

    // key1-value1, key2-value2 Insert
    InsertRow(kvssd, key1, value1);
    InsertRow(kvssd, key2, value2);

    // Read key1, key2 순차적으로 수행
    ReadRow(kvssd, key1, output_value);
    PrintFieldVector(output_value);
    ReadRow(kvssd, key2, output_value);
    PrintFieldVector(output_value);

    // key1의 value를 value2로 Update
    UpdateRow(kvssd, key1, value2);

    // Read key1, key2 순차적으로 수행
    ReadRow(kvssd, key1, output_value);
    PrintFieldVector(output_value);
    ReadRow(kvssd, key2, output_value);
    PrintFieldVector(output_value);

    // key2에 해당하는 row Delete
    DeleteRow(kvssd, key2);

    // Read key1, key2 순차적으로 수행
    ReadRow(kvssd, key1, output_value);
    PrintFieldVector(output_value);
    // key not found 에러 나는지 확인
    ReadRow(kvssd, key2, output_value);
    PrintFieldVector(output_value);

    printf("[Debug] Test ended\n");
    return 0;
}
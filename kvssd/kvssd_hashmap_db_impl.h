#ifndef YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_
#define YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_

#include "core/db.h"
#include "kvssd_hashmap_db.h"
#include "utils/utils.h"

#include <memory>

namespace ycsbc
{
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

    struct kvs_row
    {
        std::unique_ptr<kvs_key> key;
        std::unique_ptr<kvs_value> value;
    };

    void SerializeRow(const std::vector<DB::Field> &values, std::string *data);
    void DeserializeRow(std::vector<DB::Field> *values, const char *data_ptr, size_t data_len);

    std::unique_ptr<kvs_row> CreateRow(const std::string &key_in, const std::vector<DB::Field> &value_in);

    void PrintRow(const kvs_value &value);
    void PrintFieldVector(const std::vector<DB::Field> &value);

    void CheckAPI(kvs_result ret);

    // Wrapper 함수 4가지
    void ReadRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, std::vector<DB::Field> &value);
    void InsertRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<DB::Field> &value);
    void UpdateRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key, const std::vector<DB::Field> &value);
    void DeleteRow(const std::unique_ptr<KVSSD> &kvssd, const std::string &key);

} // namespace ycsbc

#endif // YCSB_C_KVSSD_HASHMAP_DB_IMPL_H_
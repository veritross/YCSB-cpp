#include "gtest/gtest.h"
#include "kvssd_hashmap_db_impl.h"

#include <ctime>
#include <string>

#define NUM_KEYS 100'000
#define NUM_VALUES 100'000

namespace
{
    class KvssdHashMapDbImplTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            kvssd.reset(new kvssd_hashmap::Hashmap_KVSSD());
        }

        std::unique_ptr<kvssd_hashmap::KVSSD> kvssd;
        std::vector<ycsbc::DB::Field> output_value;
    };

    std::string RandomPrintStr(size_t length)
    {

        std::string randomString;
        randomString.reserve(length);
        for (size_t i = 0; i < length; ++i)
        {
            randomString += ycsbc::utils::RandomPrintChar();
        }
        return randomString;
    }

    std::vector<std::string> key = []
    {
        std::vector<std::string> keys(NUM_KEYS);
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            keys[i] = "key" + std::to_string(i);
        }
        return keys;
    }();

    std::vector<std::vector<ycsbc::DB::Field>> value = []
    {
        srand(static_cast<unsigned>(time(0)));
        std::vector<std::vector<ycsbc::DB::Field>> values(NUM_VALUES);
        for (size_t i = 0; i < NUM_VALUES; i++)
        {
            values[i] = {
                {"field" + std::to_string(i) + "_1", "value" + std::to_string(i) + RandomPrintStr(32)},
                {"field" + std::to_string(i) + "_2", "value" + std::to_string(i) + RandomPrintStr(32)},
                {"field" + std::to_string(i) + "_3", "value" + std::to_string(i) + RandomPrintStr(32)}};
        }
        return values;
    }();

    bool FieldVectorCmp(const std::vector<ycsbc::DB::Field> &value1, 
                        const std::vector<ycsbc::DB::Field> &value2) {
        if (value1.size() != value2.size()) {
            return true;
        }
        for (size_t i = 0; i < value1.size(); i++) {
            if (value1[i].name != value2[i].name || value1[i].value != value2[i].value) {
                return true;
            }
        }
        return false;
    }

    struct ThreadArgs {
        const std::unique_ptr<kvssd_hashmap::KVSSD>* kv; 
        const std::vector<std::string>* keys; 
        const std::vector<std::vector<ycsbc::DB::Field>>* values;
        size_t start_idx;
        size_t end_idx;
        bool success;
        std::string error_msg; 
    };

    void* ParallelInsertUpdateRead(void* arg) {
        ThreadArgs* args = static_cast<ThreadArgs*>(arg);
        auto& kv_ref = *(args->kv);
        auto& ks = *(args->keys);
        auto& vals = *(args->values);
        args->success = true; 

        try {
            // Insert
            for (size_t i = args->start_idx; i < args->end_idx; i++) {
                kvssd_hashmap::InsertRow(kv_ref, ks[i], vals[i]);
            }

            // Update
            size_t range = args->end_idx - args->start_idx;
            for (size_t i = args->start_idx; i < args->end_idx; i++) {
                size_t update_idx = args->start_idx + ((i - args->start_idx + 100) % range);
                kvssd_hashmap::UpdateRow(kv_ref, ks[i], vals[update_idx]);
            }

            // Read & Check
            std::vector<ycsbc::DB::Field> output_val;
            for (size_t i = args->start_idx; i < args->end_idx; i++) {
                size_t check_idx = args->start_idx + ((i - args->start_idx + 100) % range);
                kvssd_hashmap::ReadRow(kv_ref, ks[i], output_val);

                if (FieldVectorCmp(output_val, vals[check_idx])) {
                    args->success = false;
                    args->error_msg = "Data mismatch for key: " + ks[i];
                    return nullptr;
                }
            }
        } catch (const ycsbc::utils::Exception& e) {
            args->success = false;
            args->error_msg = std::string("Caught ycsbc::utils::Exception: ") + e.what();
        } catch (const std::exception& e) {
            args->success = false;
            args->error_msg = std::string("Caught std::exception: ") + e.what();
        } catch (...) {
            args->success = false;
            args->error_msg = "Caught unknown exception";
        }

        return nullptr;
    }

    TEST_F(KvssdHashMapDbImplTest, ReadSmall)
    {
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, ReadLarge)
    {
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, Reinsertion)
    {
        EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[0], value[0]));
        EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[1], value[1]));
        EXPECT_THROW({
        try
        {
            kvssd_hashmap::InsertRow(kvssd, key[0], value[2]);
        }
        catch( const ycsbc::utils::Exception& e )
        {
            EXPECT_STREQ( "Key space is already created", e.what() );
            throw;
        } }, ycsbc::utils::Exception);
        EXPECT_THROW({
        try
        {
            kvssd_hashmap::InsertRow(kvssd, key[1], value[3]);
        }
        catch( const ycsbc::utils::Exception& e )
        {
            EXPECT_STREQ( "Key space is already created", e.what() );
            throw;
        } }, ycsbc::utils::Exception);
    }

    TEST_F(KvssdHashMapDbImplTest, UpdateSmall)
    {
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::UpdateRow(kvssd, key[i], value[i + 10]));
        }
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[i + 10], output_value));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, UpdateLarge)
    {
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::UpdateRow(kvssd, key[i], value[(i + 500) % NUM_KEYS]));
        }
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[(i + 500) % NUM_KEYS], output_value));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, UpdateAccessInvalidKey)
    {
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        EXPECT_THROW({
        try
        {
            kvssd_hashmap::UpdateRow(kvssd, key[99], value[99]);
        }
        catch( const ycsbc::utils::Exception& e )
        {
            EXPECT_STREQ( "Key space does not exist", e.what() );
            throw;
        } }, ycsbc::utils::Exception);
    }

    TEST_F(KvssdHashMapDbImplTest, DeleteSmall)
    {
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
            EXPECT_NO_THROW(kvssd_hashmap::DeleteRow(kvssd, key[i]));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, DeleteLarge)
    {
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < NUM_KEYS; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
            EXPECT_NO_THROW(kvssd_hashmap::DeleteRow(kvssd, key[i]));
        }
    }

    TEST_F(KvssdHashMapDbImplTest, DeleteAccessInvalidKey)
    {
        for (size_t i = 0; i < 10; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        EXPECT_THROW({
        try
        {
            kvssd_hashmap::DeleteRow(kvssd, key[99]);
        }
        catch( const ycsbc::utils::Exception& e )
        {
            EXPECT_STREQ( "Key space does not exist", e.what() );
            throw;
        } }, ycsbc::utils::Exception);
    }

    TEST_F(KvssdHashMapDbImplTest, ParallelOperations) {
        const size_t NUM_THREADS = 4;
        pthread_t threads[NUM_THREADS];
        ThreadArgs thread_args[NUM_THREADS];

        size_t range_per_thread = NUM_KEYS / NUM_THREADS;

        for (size_t t = 0; t < NUM_THREADS; t++) {
            thread_args[t].kv = &kvssd;
            thread_args[t].keys = &key;
            thread_args[t].values = &value;
            thread_args[t].start_idx = t * range_per_thread;
            thread_args[t].end_idx = (t == NUM_THREADS - 1) ? NUM_KEYS : (t + 1) * range_per_thread;
            thread_args[t].success = false;
            thread_args[t].error_msg = "";
        }

        for (size_t t = 0; t < NUM_THREADS; t++) {
            int ret = pthread_create(&threads[t], nullptr, ParallelInsertUpdateRead, &thread_args[t]);
            ASSERT_EQ(ret, 0) << "pthread_create failed on thread " << t;
        }

        for (size_t t = 0; t < NUM_THREADS; t++) {
            int ret = pthread_join(threads[t], nullptr);
            ASSERT_EQ(ret, 0) << "pthread_join failed on thread " << t;
        }

        for (size_t t = 0; t < NUM_THREADS; t++) {
            EXPECT_TRUE(thread_args[t].success) << "Thread " << t << " operations failed. Error: " << thread_args[t].error_msg;
        }
    }
} // anonymous namespace
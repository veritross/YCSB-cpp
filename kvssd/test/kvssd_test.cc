#include "gtest/gtest.h"
#include "kvssd_hashmap_db_impl.h"

#include <ctime>
#include <string>

#define NUM_KEYS 100000
#define NUM_VALUES 100000

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

    bool FieldVectorCmp(std::vector<ycsbc::DB::Field> &value1, std::vector<ycsbc::DB::Field> &value2)
    {
        size_t len1 = value1.size();
        size_t len2 = value2.size();
        if (len1 != len2)
        {
            return true;
        }
        for (size_t i = 0; i < len1; i++)
        {
            if (value1[i].name != value2[i].name)
            {
                return true;
            }
            if (value1[i].value != value2[i].value)
            {
                return true;
            }
        }
        return false;
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
        for (size_t i = 0; i < 100'000; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 100'000; i++)
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
        for (size_t i = 0; i < 100'000; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 100'000; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::UpdateRow(kvssd, key[i], value[(i + 500) % 100'000]));
        }
        for (size_t i = 0; i < 100'000; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::ReadRow(kvssd, key[i], output_value));
            EXPECT_FALSE(FieldVectorCmp(value[(i + 500) % 100'000], output_value));
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
        for (size_t i = 0; i < 100'000; i++)
        {
            EXPECT_NO_THROW(kvssd_hashmap::InsertRow(kvssd, key[i], value[i]));
        }
        for (size_t i = 0; i < 100'000; i++)
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
} // anonymous namespace
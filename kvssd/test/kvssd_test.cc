#include "gtest/gtest.h"
#include "../kvssd_hashmap_db_impl.h"

#include <ctime>
#include <string>

#define NUM_KEYS 1000
#define NUM_VALUES 1000

using namespace ycsbc;

std::string RandomPrintStr(size_t length)
{

    std::string randomString;
    randomString.reserve(length);
    for (size_t i = 0; i < length; ++i)
    {
        randomString += utils::RandomPrintChar();
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

std::vector<std::vector<DB::Field>> value = []
{
    srand(static_cast<unsigned>(time(0)));
    std::vector<std::vector<DB::Field>> values(NUM_VALUES);
    for (size_t i = 0; i < NUM_VALUES; i++)
    {
        values[i] = {
            {"field" + std::to_string(i) + "_1", "value" + std::to_string(i) + RandomPrintStr(32)},
            {"field" + std::to_string(i) + "_2", "value" + std::to_string(i) + RandomPrintStr(32)},
            {"field" + std::to_string(i) + "_3", "value" + std::to_string(i) + RandomPrintStr(32)}};
    }
    return values;
}();

std::vector<DB::Field> output_value;

std::unique_ptr<KVSSD> kvssd(NewKvssdDB());

bool FieldVectorCmp(std::vector<DB::Field> &value1, std::vector<DB::Field> &value2)
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

// ----------------------- TEST START ----------------------- //

TEST(READ, Simple)
{
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
    }
}

TEST(READ, Large)
{
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
    }
}

TEST(INSERT, Reinsertion)
{
    EXPECT_NO_THROW(InsertRow(kvssd, key[0], value[0]));
    EXPECT_NO_THROW(InsertRow(kvssd, key[1], value[1]));
    EXPECT_THROW({
        try
        {
            InsertRow(kvssd, key[0], value[2]);
        }
        catch( const utils::Exception& e )
        {
            EXPECT_STREQ( "Key space is already created", e.what() );
            throw;
        } }, utils::Exception);
    EXPECT_THROW({
        try
        {
            InsertRow(kvssd, key[1], value[3]);
        }
        catch( const utils::Exception& e )
        {
            EXPECT_STREQ( "Key space is already created", e.what() );
            throw;
        } }, utils::Exception);
}

TEST(Update, Simple)
{
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(UpdateRow(kvssd, key[i], value[i + 10]));
    }
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[i + 10], output_value));
    }
}

TEST(Update, Large)
{
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(UpdateRow(kvssd, key[i], value[(i + 500) % 1000]));
    }
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[(i + 500) % 1000], output_value));
    }
}

TEST(Update, AccessInvalidKey)
{
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    EXPECT_THROW({
        try
        {
            UpdateRow(kvssd, key[99], value[99]);
        }
        catch( const utils::Exception& e )
        {
            EXPECT_STREQ( "Key space does not exist", e.what() );
            throw;
        } }, utils::Exception);
}

TEST(Delete, Simple)
{
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
        EXPECT_NO_THROW(DeleteRow(kvssd, key[i]));
    }
}

TEST(Delete, Large)
{
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    for (size_t i = 0; i < 1000; i++)
    {
        EXPECT_NO_THROW(ReadRow(kvssd, key[i], output_value));
        EXPECT_FALSE(FieldVectorCmp(value[i], output_value));
        EXPECT_NO_THROW(DeleteRow(kvssd, key[i]));
    }
}

TEST(Delete, AccessInvalidKey)
{
    for (size_t i = 0; i < 10; i++)
    {
        EXPECT_NO_THROW(InsertRow(kvssd, key[i], value[i]));
    }
    EXPECT_THROW({
        try
        {
            DeleteRow(kvssd, key[99]);
        }
        catch( const utils::Exception& e )
        {
            EXPECT_STREQ( "Key space does not exist", e.what() );
            throw;
        } }, utils::Exception);
}
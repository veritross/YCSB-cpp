#include "kvssd.h"
#include "kvssd_hashmap_db.h"

#include "core/db_factory.h"

namespace
{
    const std::string PROP_BACKEND = "kvssd.backend";
    const std::string PROP_BACKEND_DEFAULT = "kvssd";
} // anonymous namespace

ycsbc::DB *NewKvssdDB()
{
    std::string backend = PROP_BACKEND;
    ycsbc::DB *ret = nullptr;
    if (backend == "hashmap")
    {
        ret = new kvssd_hashmap::Hashmap_KVSSD();
    }
    if (backend == "kvssd")
    {
        // ret = new kvssd::KVSSD();
    }
    return ret;
}

const bool registered = ycsbc::DBFactory::RegisterDB("kvssd", NewKvssdDB);
#include "kvssd_hashmap_db.h"

#include <string>

namespace kvssd_hashmap {

Hashmap_KVSSD::Hashmap_KVSSD() { pthread_rwlock_init(&rwl, nullptr); }
Hashmap_KVSSD::~Hashmap_KVSSD() {
    pthread_rwlock_wrlock(&rwl);
    for (auto &[key, value] : db) {
        if (key.key != nullptr) {
            free(key.key);
        }
        if (value.value != nullptr) {
            free(value.value);
        }
    }
    db.clear();
    pthread_rwlock_unlock(&rwl);
    pthread_rwlock_destroy(&rwl);
}

kvssd::kvs_result Hashmap_KVSSD::ValidateRequest(
    const kvssd::kvs_key &key,
    std::optional<std::reference_wrapper<const kvssd::kvs_value>> value) const {
    if (key.length < KVS_MIN_KEY_LENGTH || KVS_MAX_KEY_LENGTH < key.length) {
        return kvssd::kvs_result::KVS_ERR_KEY_LENGTH_INVALID;
    }
    if (key.key == nullptr) {
        return kvssd::kvs_result::KVS_ERR_PARAM_INVALID;
    }
    if (value) {
        if (value->get().length < KVS_MIN_VALUE_LENGTH ||
            KVS_MAX_VALUE_LENGTH < value->get().length) {
            return kvssd::kvs_result::KVS_ERR_VALUE_LENGTH_INVALID;
        }
        if (value->get().offset & (KVS_ALIGNMENT_UNIT - 1)) {
            return kvssd::kvs_result::KVS_ERR_VALUE_OFFSET_MISALIGNED;
        }
        if (value->get().value == nullptr && value->get().length) {
            return kvssd::kvs_result::KVS_ERR_PARAM_INVALID;
        }
    }
    return kvssd::kvs_result::KVS_SUCCESS;
}

kvssd::kvs_key Hashmap_KVSSD::DeepCopyKey(const kvssd::kvs_key &orig) const {
    kvssd::kvs_key copy;
    copy.length = orig.length;
    copy.key = malloc(orig.length);
    if (copy.key && orig.key) {
        std::memcpy(copy.key, orig.key, orig.length);
    }
    return copy;
}

kvssd::kvs_value Hashmap_KVSSD::DeepCopyValue(const kvssd::kvs_value &orig) const {
    kvssd::kvs_value copy;
    copy.length = orig.length;
    copy.actual_value_size = orig.actual_value_size;
    copy.offset = orig.offset;
    copy.value = malloc(orig.length);
    if (copy.value && orig.value) {
        std::memcpy(copy.value, orig.value, orig.length);
    }
    return copy;
}

// API Functions
kvssd::kvs_result Hashmap_KVSSD::Read(const kvssd::kvs_key &key, kvssd::kvs_value &value_out) {
    if (kvssd::kvs_result ret = ValidateRequest(key, value_out);
        ret != kvssd::kvs_result::KVS_SUCCESS) {
        return ret;
    }
    pthread_rwlock_rdlock(&rwl);
    auto it = db.find(key);
    pthread_rwlock_unlock(&rwl);
    if (it == db.end()) {
        return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
    }
    value_out = DeepCopyValue(it->second);
    return kvssd::kvs_result::KVS_SUCCESS;
}

kvssd::kvs_result Hashmap_KVSSD::Insert(const kvssd::kvs_key &key, const kvssd::kvs_value &value) {
    if (kvssd::kvs_result ret = ValidateRequest(key, value);
        ret != kvssd::kvs_result::KVS_SUCCESS) {
        return ret;
    }
    pthread_rwlock_wrlock(&rwl);
    if (auto it = db.find(key); it != db.end()) {
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_ERR_KS_EXIST;
    }
    kvssd::kvs_key key_copy = DeepCopyKey(key);
    kvssd::kvs_value value_copy = DeepCopyValue(value);

    db.try_emplace(key_copy, value_copy);
    pthread_rwlock_unlock(&rwl);
    return kvssd::kvs_result::KVS_SUCCESS;
}

kvssd::kvs_result Hashmap_KVSSD::Update(const kvssd::kvs_key &key, const kvssd::kvs_value &value) {
    if (kvssd::kvs_result ret = ValidateRequest(key, value);
        ret != kvssd::kvs_result::KVS_SUCCESS) {
        return ret;
    }
    pthread_rwlock_wrlock(&rwl);
    auto it = db.find(key);
    if (it == db.end()) {
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
    }

    if (it->second.value != nullptr) {
        free(it->second.value);
        it->second.value = nullptr;
    }

    kvssd::kvs_value value_copy = DeepCopyValue(value);
    it->second = value_copy;
    pthread_rwlock_unlock(&rwl);
    return kvssd::kvs_result::KVS_SUCCESS;
}

kvssd::kvs_result Hashmap_KVSSD::Delete(const kvssd::kvs_key &key) {
    if (kvssd::kvs_result ret = ValidateRequest(key, std::nullopt);
        ret != kvssd::kvs_result::KVS_SUCCESS) {
        return ret;
    }
    pthread_rwlock_wrlock(&rwl);
    if (auto it = db.find(key); it == db.end()) {
        pthread_rwlock_unlock(&rwl);
        return kvssd::kvs_result::KVS_ERR_KS_NOT_EXIST;
    }
    db.erase(key);
    pthread_rwlock_unlock(&rwl);
    return kvssd::kvs_result::KVS_SUCCESS;
}

}  // namespace kvssd_hashmap

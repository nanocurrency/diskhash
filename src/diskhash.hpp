#ifndef DISKHASH_HPP_INCLUDE_GUARD__
#define DISKHASH_HPP_INCLUDE_GUARD__

#include "diskhash.h"
#include "os_wrappers.h"

#include <cinttypes>
#include <cassert>
#include <stdexcept>
#include <type_traits>
#include <string>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace dht {
enum OpenMode { DHOpenRO, DHOpenRW, DHOpenRWNoCreate };

template <typename T>
struct DiskHash {
    static_assert(std::is_trivially_copyable<T>::value,
            "DiskHash only works for POD (plain old data) types that can be mempcy()ed around");
private:
    std::string db_file_path;
    enum OpenMode open_mode;
    int key_size;

    void instantiate_table (const char* fname, const int keysize, OpenMode m) {
        char* err = nullptr;
        int flags;
        if (m == DHOpenRO) {
            flags = O_RDONLY;
        } else if (m == DHOpenRW) {
            flags = O_RDWR|O_CREAT;
        } else {
            flags = O_RDWR;
        }
        HashTableOpts opts;
        opts.key_maxlen = keysize;
        opts.object_datalen = sizeof(T);
        ht_ = dht_open(fname, opts, flags, &err);
        if (!ht_) {
            if (!err) throw std::bad_alloc();
            std::string error = "Error opening file '" + std::string(fname) + "': " + std::string(err);
            std::free(err);
            throw std::runtime_error(error);
        }
    }

public:
    /***
     * Open a diskhash from disk
     */
    DiskHash(const char* fname, const int keysize, OpenMode m) :
        ht_(0),
        db_file_path (fname),
        key_size (keysize),
        open_mode (m)
    {
        instantiate_table (db_file_path.c_str(), key_size, open_mode);
    }

    DiskHash(DiskHash&& other) :
        ht_(other.ht_),
        db_file_path (other.db_file_path),
        key_size (other.key_size)
    {
        other.ht_ = 0;
        other.db_file_path = "";
        other.key_size = 0;
    }

    ~DiskHash() {
        if (ht_) dht_free(ht_);
    }

    /**
     * Check if key is a member
     */
    bool is_member(const char* key) const { return const_cast<DiskHash<T>*>(this)->lookup(key); }

    /**
     * Return a pointer to the element (if present, otherwise nullptr).
     *
     * Note that if the diskhash was not opened in read-write mode, then
     * the memory will not be writeable.
     */
    T* lookup(const char* key) {
        if (!ht_) return nullptr;
        return static_cast<T*>(dht_lookup(ht_, key));
    }

    /**
     * Delete an element.
     *
     * Returns true when the deletion is done. Returns false when the key is
     * not found.
     *
     * Throws an exception when the informed arguments are invalid or when
     * the hash table is inconsistent (this behaviour must never happen).
     */
    bool remove(const char* key) {
        if (!ht_) return false;
        char* err = nullptr;
        const int ret_delete = dht_delete(ht_, key, &err);
        if (ret_delete == 1) {
            std::free(err);
            return true;
        }
        if (ret_delete == 0) {
            std::free(err);
            return false;
        }
        auto error = std::string(err);
        if (ret_delete == -EINVAL) {
            std::free(err);
            throw std::invalid_argument(error);
        }
        std::free(err);
        throw std::runtime_error(error);
    }

    /**
     * Insert an element
     *
     * Returns true if element was inserted (else false and nothing is
     * modified).
     */
    bool insert(const char* key, const T& val) {
        char* err = nullptr;
        const int icode = dht_insert(ht_, key, &val, &err);
        if (icode <= 0) return false;
        if (icode == 1) return true;
        std::string error ("Error: " + std::string(err));
        std::free(err);
        throw std::runtime_error(error);
    }

    /**
     * Insert an element
     *
     * Returns true if element was inserted (else false and nothing is
     * modified).
     */
    bool insert(const char* key, const void* val) {
        char* err = nullptr;
        const int icode = dht_insert(ht_, key, val, &err);
        if (icode <= 0) return false;
        if (icode == 1) return true;
        std::string error ("Error: " + std::string(err));
        std::free(err);
        throw std::runtime_error(error);
    }

    /**
     * Update an element
     *
     * Returns true if the element was updated (else false and nothing
     * is modified).
     */
    bool update(const char* key, const T& val) {
        char* err = nullptr;
        const int icode = dht_update(ht_, key, &val, &err);
        if (icode == 0) return false;
        if (icode == 1) return true;
        auto error ("Error: " + std::string(err));
        std::free(err);
        throw std::runtime_error(error);
    }

    /**
     * Reserve space.
     *
     * Pre-allocates space in the hash table.
     *
     * This function is useful to avoid data reallocation procedures everytime
     * the capacity is increased.
     */
     void reserve(unsigned long capacity) {
        char* err = nullptr;
        if (capacity < size()) {
            return;
        }
        const int reserve_return = dht_reserve(ht_, capacity, &err);
        if (reserve_return >= 1) {
            return;
        }
        if (!err) { throw std::bad_alloc(); }
        std::string error = "Error pre-allocating space to capacity='"
                + std::to_string(capacity)
                + "': " + std::string(err);
        std::free(err);
        throw std::runtime_error(error);
     }

    /**
     * Returns the table's size.
     */
    unsigned long size() {
        return (unsigned long) dht_size(ht_);
    }

    /**
     * Returns the current table's capacity.
     */
     unsigned long capacity() {
         return (unsigned long) dht_capacity(ht_);
     }

    /**
     * Returns the number of soft-deleted slots.
     */
    unsigned long dirty_slots() {
         return (unsigned long) dht_dirty_slots(ht_);
     }

    /**
     * Closes/frees resources allocated to the current table.
     * Deletes the table file on the disk and instantiates a clean table.
     */
    void clear() {
        dht_free (ht_);
        dht_delete_file (db_file_path.c_str ());
        instantiate_table (db_file_path.c_str (), key_size, open_mode);
    }

    DiskHash(const DiskHash&) = delete;
    DiskHash& operator=(const DiskHash&) = delete;

    struct iterator;

    iterator begin() const {
        return iterator(0, *this);
    }

    iterator end() const {
        return iterator(used_slots(), *this);
    }

private:
    /**
     * Returns the number of used slots.
     */
    unsigned long used_slots() const {
        return (unsigned long) dht_slots_used(ht_);
    }

    HashTable* ht_;
};

}

#endif /* DISKHASH_HPP_INCLUDE_GUARD__ */

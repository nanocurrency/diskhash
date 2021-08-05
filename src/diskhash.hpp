#ifndef DISKHASH_HPP_INCLUDE_GUARD__
#define DISKHASH_HPP_INCLUDE_GUARD__

#include "diskhash.h"
#include "os_wrappers.h"

#include <cinttypes>
#include <stdexcept>
#include <type_traits>

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
        instantiate_table (fname, keysize, m);
    }
    DiskHash(DiskHash&& other):ht_(other.ht_) { other.ht_ = 0; }

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
        if (!err) { throw std::bad_alloc(); }
        std::string error = "Error inserting key '" + std::string(key) + "': " + std::string(err);
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
private:
    HashTable* ht_;
};

}

#endif /* DISKHASH_HPP_INCLUDE_GUARD__ */

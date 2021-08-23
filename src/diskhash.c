#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdbool.h>

#include "diskhash.h"
#include "os_wrappers.h"
#include "primes.h"
#include "rtable.h"

static const size_t INITIAL_SIZE = 7;
static const size_t INITIAL_CAPACITY = 3;

enum {
    HT_FLAG_CAN_WRITE = 1,
    HT_FLAG_HASH_2 = 2,
    HT_FLAG_IS_LOADED = 4,
};

typedef struct HashTableHeader {
    char magic[16];
    HashTableOpts opts_;
    size_t cursize_;
    size_t slots_used_;
} HashTableHeader;

typedef struct HashTableEntry {
    const char* ht_key;
    void* ht_data;
} HashTableEntry;

static
uint64_t hash_key(const char* k, int use_hash_2) {
    /* Taken from http://www.cse.yorku.ca/~oz/hash.html */
    const unsigned char* ku = (const unsigned char*)k;
    uint64_t hash = 5381;
    uint64_t next;
    for ( ; *ku; ++ku) {
        hash *= 33;
        next = *ku;
        if (use_hash_2) {
            next = rtable[next];
        }
        hash ^= next;
    }
    return hash;
}

inline static
size_t aligned_size(size_t s) {
    size_t s_8bytes = s & ~0x7;
    return s_8bytes == s ? s : (s_8bytes + 8);
}

inline static
HashTableHeader* header_of(HashTable* ht) {
    return (HashTableHeader*)ht->data_;
}

inline static
const HashTableHeader* cheader_of(const HashTable* ht) {
    return (const HashTableHeader*)ht->data_;
}

inline static
int is_64bit(const HashTable* ht) {
    return cheader_of(ht)->cursize_ > (1L << 32);
}

inline static
size_t node_size_opts(HashTableOpts opts) {
    return aligned_size(opts.key_maxlen + 1) + aligned_size(opts.object_datalen);
}

inline static
size_t node_size(const HashTable* ht) {
    return node_size_opts(cheader_of(ht)->opts_);
}

inline static
int entry_empty(const HashTableEntry et) {
    return !et.ht_key;
}

void* hashtable_of(HashTable* ht) {
    return (unsigned char*)ht->data_ + sizeof(HashTableHeader);
}

static
uint64_t get_table_at(const HashTable* ht, uint64_t hash) {
    assert(hash < cheader_of(ht)->cursize_);
    if (is_64bit(ht)) {
        uint64_t* table = (uint64_t*)hashtable_of((HashTable*)ht);
        return table[hash];
    } else {
        uint32_t* table = (uint32_t*)hashtable_of((HashTable*)ht);
        return table[hash];
    }
}

static
void set_table_at(HashTable* ht, uint64_t hash, const uint64_t val) {
    if (is_64bit(ht)) {
        uint64_t* table = (uint64_t*)hashtable_of(ht);
        table[hash] = val;
    } else {
        uint32_t* table = (uint32_t*)hashtable_of(ht);
        table[hash] = val;
    }
}

void show_ht(const HashTable* ht) {
    fprintf(stderr, "HT {\n"
                "\tmagic = \"%s\",\n"
                "\tcursize = %d,\n"
                "\tslots used = %zd\n"
                "\n", cheader_of(ht)->magic, (int)cheader_of(ht)->cursize_, cheader_of(ht)->slots_used_);

    uint64_t i;
    for (i = 0; i < cheader_of(ht)->cursize_; ++i) {
        fprintf(stderr, "\tTable [ %d ] = %d\n",(int)i, (int)get_table_at(ht, i));
    }
    fprintf(stderr, "}\n");
}

static
HashTableEntry entry_by_index(const HashTable* ht, size_t ix) {
    HashTableEntry r;
    if (ix == 0) {
        r.ht_key = 0;
        r.ht_data = 0;
        return r;
    }
    --ix;
    const size_t sizeof_table_elem = is_64bit(ht) ? sizeof(uint64_t) : sizeof(uint32_t);
    const char* node_data = (const char*)ht->data_
                            + sizeof(HashTableHeader)
                            + cheader_of(ht)->cursize_ * sizeof_table_elem;
    r.ht_key = node_data + ix * node_size(ht);
    r.ht_data = (void*)( node_data + ix * node_size(ht) + aligned_size(cheader_of(ht)->opts_.key_maxlen + 1) );
    return r;
}

static
HashTableEntry entry_at(const HashTable* ht, size_t hash) {
    size_t ix = get_table_at(ht, hash);
    return entry_by_index(ht, ix);
}

HashTableOpts dht_zero_opts() {
    HashTableOpts r;
    r.key_maxlen = 0;
    r.object_datalen = 0;
    return r;
}

HashTable* dht_open(const char* fpath, HashTableOpts opts, int flags, char** err) {
    if (!fpath || !*fpath) return NULL;
    const dht_file_t fd = dht_open_file(fpath, flags, false);
    int needs_init = 0;
    bool fd_err = false;
#ifdef _WIN32
    fd_err = fd == NULL;
#else
    fd_err = fd < 0;
#endif
    if (fd_err) {
        if (err) { *err = strdup("open call failed."); }
        return NULL;
    }
    HashTable* rp = (HashTable*)malloc(sizeof(HashTable));
    if (!rp) {
        if (err) { *err = NULL; }
        return NULL;
    }
    rp->fd_ = fd;
    rp->fname_ = strdup(fpath);
    if (!rp->fname_) {
        if (err) { *err = NULL; }
        dht_close_file(rp->fd_);
        free(rp);
        return NULL;
    }
    dht_file_size(rp->fd_, &rp->datasize_);
    if (rp->datasize_ == 0) {
        needs_init = 1;
        rp->datasize_ = sizeof(HashTableHeader) + INITIAL_SIZE * sizeof(uint32_t) + INITIAL_CAPACITY * node_size_opts(opts);
        if (!dht_truncate_file(fd, rp->datasize_)) {
            if (err) {
                *err = malloc(256);
                if (*err) {
                    snprintf(*err, 256, "Could not allocate disk space. Error: %s.", strerror(errno));
                }
            }
            dht_close_file(rp->fd_);
            free((char*)rp->fname_);
            free(rp);
            return NULL;
        }
    }
    rp->flags_ = HT_FLAG_HASH_2;
    const int prot = (flags == O_RDONLY) ?
                                PROT_READ
                                : PROT_READ|PROT_WRITE;
    if (prot & PROT_WRITE) rp->flags_ |= HT_FLAG_CAN_WRITE;
    bool map_success = dht_memory_map_file(rp->fd_, &rp->data_, rp->datasize_, prot);
    if (!map_success) {
        if (err) { *err = strdup("mmap() call failed."); }
        dht_close_file(rp->fd_);
        free((char*)rp->fname_);
        free(rp);
        return NULL;
    }
    if (needs_init) {
        strcpy(header_of(rp)->magic, "DiskBasedHash11");
        header_of(rp)->opts_ = opts;
        header_of(rp)->cursize_ = INITIAL_SIZE;
        header_of(rp)->slots_used_ = 0;
    } else if (strcmp(header_of(rp)->magic, "DiskBasedHash11")) {
        if (!strcmp(header_of(rp)->magic, "DiskBasedHash10")) {
            rp->flags_ &= ~HT_FLAG_HASH_2;
        } else {
            char start[16];
            strncpy(start, header_of(rp)->magic, 14);
            start[13] = '\0';
            if (!strcmp(start, "DiskBasedHash")) {
                if (err) { *err = strdup("Version mismatch. This code can only load version 1.0 or 1.1."); }
            } else {
                if (err) { *err = strdup("No magic number found."); }
            }
            dht_free(rp);
            return 0;
        }
    } else if ((header_of(rp)->opts_.key_maxlen != opts.key_maxlen && opts.key_maxlen != 0)
                || (header_of(rp)->opts_.object_datalen != opts.object_datalen && opts.object_datalen != 0)) {
        if (err) { *err = strdup("Options mismatch (diskhash table on disk was not created with the same options used to open it)."); }
        dht_free(rp);
        return 0;
    }
    return rp;
}

int dht_load_to_memory(HashTable* ht, char** err) {
    if (ht->flags_ & HT_FLAG_CAN_WRITE) {
        if (err) *err = "Cannot call dht_load_to_memory on a read/write Diskhash";
        return 1;
    }
    if (ht->flags_ & HT_FLAG_IS_LOADED) {
        if (err) *err = "dht_load_to_memory had already been called.";
        return 1;
    }
    dht_memory_unmap_file(ht->data_, ht->datasize_);
    ht->data_ = malloc(ht->datasize_);
    if (ht->data_) {
        size_t n = (size_t) dht_read_file(ht->fd_, ht->data_, ht->datasize_);
        if (n == ht->datasize_) {
            ht->flags_ |= HT_FLAG_IS_LOADED;
            return 0;
        }
        else if (err) *err = "dht_load_to_memory: could not read data from file";
    } else {
        if (err) *err = "dht_load_to_memory: could not allocate memory.";
    }
    free(ht->data_);
    dht_file_sync(ht->fd_);
    dht_close_file(ht->fd_);
    free((char*)ht->fname_);
    free(ht);
    return 2;
}

void dht_free(HashTable* ht) {
    if (ht->flags_ & HT_FLAG_IS_LOADED) {
        free(ht->data_);
    } else {
        dht_memory_unmap_file(ht->data_, ht->datasize_);
    }
    dht_file_sync(ht->fd_);
    dht_close_file(ht->fd_);
    free((char*)ht->fname_);
    free(ht);
}

char random_char(void) {
    const char* available =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return available[rand() % (26*2 + 10)];
}


char* generate_tempname_from(const char* base) {
    char* res = (char*)malloc(strlen(base) + 21);
    if (!res) return NULL;
    strcpy(res, base);
    char* p = res;
    while (*p) ++p;
    *p++ = '.';
    int i;
    for (i = 0; i < 19; ++i) {
        *p++ = random_char();
    }
    *p = 0;
    return res;
}

size_t dht_reserve(HashTable* ht, size_t cap, char** err) {
    if (!ht) {
        if (err) { *err = strdup("Invalid null HashTable object."); }
        return -EINVAL;
    }
    if (!(ht->flags_ & HT_FLAG_CAN_WRITE)) {
        if (err) { *err = strdup("Hash table is read-only. Cannot call dht_reserve."); }
        return -EACCES;
    }
    if (header_of(ht)->cursize_ / 2 > cap) {
        return header_of(ht)->cursize_ / 2;
    }
    const uint64_t starting_slots = cheader_of(ht)->slots_used_;
    const uint64_t min_slots = cap * 2 + 1;
    uint64_t i = 0;
    while (primes[i] && primes[i] < min_slots) ++i;
    const uint64_t n = primes[i];
    cap = n / 2;
    const size_t sizeof_table_elem = is_64bit(ht) ? sizeof(uint64_t) : sizeof(uint32_t);
    const size_t total_size = sizeof(HashTableHeader) + n * sizeof_table_elem + cap * node_size(ht);

    HashTable* temp_ht = (HashTable*)malloc(sizeof(HashTable));
    if (!temp_ht) {
        if (err) { *err = strdup("dht_reserve: could not allocate memory."); }
        return 0;
    }
    while (1) {
        temp_ht->fname_ = generate_tempname_from(ht->fname_);
        if (!temp_ht->fname_) {
            if (err) { *err = strdup("dht_reserve: could not allocate memory."); }
            free(temp_ht);
            return 0;
        }
        temp_ht->fd_ = dht_open_file(temp_ht->fname_, O_EXCL | O_CREAT | O_RDWR, true);
        if (temp_ht->fd_) break;
        free((char*)temp_ht->fname_);
    }
    if (!dht_truncate_file(temp_ht->fd_, total_size)) {
        if (err) {
            *err = malloc(256);
            if (*err) {
                snprintf(*err, 256, "Could not allocate disk space. Error: %s.", strerror(errno));
            }
        }
        free((char*)temp_ht->fname_);
        free(temp_ht);
        return 0;
    }
    temp_ht->datasize_ = total_size;
    bool map_success = dht_memory_map_file(temp_ht->fd_, &temp_ht->data_, temp_ht->datasize_, PROT_READ | PROT_WRITE);
    temp_ht->flags_ = ht->flags_;
    if (!map_success) {
        if (err) {
            const int errorbufsize = 512;
            *err = (char*)malloc(errorbufsize);
            if (*err) {
                snprintf(*err, errorbufsize, "Could not mmap() new hashtable: %s.\n", strerror(errno));
            }
        }
        dht_close_file(temp_ht->fd_);
        dht_delete_file(temp_ht->fname_);
        free((char*)temp_ht->fname_);
        free(temp_ht);
        return 0;
    }
    memcpy(header_of(temp_ht), header_of(ht), sizeof(HashTableHeader));
    header_of(temp_ht)->cursize_ = n;
    header_of(temp_ht)->slots_used_ = 0;

    if (!strcmp(header_of(temp_ht)->magic, "DiskBasedHash10")) {
        strcpy(header_of(temp_ht)->magic, "DiskBasedHash11");
        temp_ht->flags_ |= HT_FLAG_HASH_2;
    }

    HashTableEntry et;
    for (i = 0; i < header_of(ht)->slots_used_; ++i) {
        set_table_at(ht, 0, i + 1);
        et = entry_at(ht, 0);
        dht_insert(temp_ht, et.ht_key, et.ht_data, NULL);
    }

    char* temp_fname = strdup(temp_ht->fname_);
    if (!temp_fname) {
        if (err) { *err = NULL; }
        dht_delete_file(temp_ht->fname_);
        dht_free(temp_ht);
        return 0;
    }

    dht_free(temp_ht);
    const HashTableOpts opts = header_of(ht)->opts_;

    dht_memory_unmap_file(ht->data_, ht->datasize_);
    dht_close_file(ht->fd_);

    rename(temp_fname, ht->fname_);
    free((char*)temp_fname);

    temp_ht = dht_open(ht->fname_, opts, O_RDWR, err);
    if (!temp_ht) {
        /* err is set by dht_open */
        return 0;
    }
    free((char*)ht->fname_);
    memcpy(ht, temp_ht, sizeof(HashTable));
    free(temp_ht);

    assert(starting_slots == cheader_of(ht)->slots_used_);
    return cap;
}

size_t dht_size(const HashTable* ht) {
    return cheader_of(ht)->slots_used_;
}

int dht_indexed_lookup (HashTable* ht, size_t index, char** key, void* data, char** err) {
    if (index >= cheader_of(ht)->slots_used_) {
        if (err) { *err = strdup("The index is out-of-range."); }
        return -EINVAL;
    }
    HashTableEntry et;
    et = entry_by_index(ht, (index + 1));
    if (!entry_empty(et)) {
        strncpy(*key, et.ht_key, cheader_of(ht)->opts_.key_maxlen);
        memcpy(data, et.ht_data, cheader_of(ht)->opts_.object_datalen);
        return 1;
    }
    if (err) { *err = strdup("The informed index doesn't contain any data."); }
    return -EFAULT;
}

void* dht_lookup(const HashTable* ht, const char* key) {
    uint64_t h = hash_key(key, ht->flags_ & HT_FLAG_HASH_2) % cheader_of(ht)->cursize_;
    uint64_t i;
    for (i = 0; i < cheader_of(ht)->cursize_; ++i) {
        HashTableEntry et = entry_at(ht, h);
        if (entry_empty(et)) return NULL;
        if (!strcmp(et.ht_key, key)) return et.ht_data;
        ++h;
        if (h == cheader_of(ht)->cursize_) h = 0;
    }
    fprintf(stderr, "dht_lookup: the code should never have reached this line.\n");
    return NULL;
}

int dht_insert(HashTable* ht, const char* key, const void* data, char** err) {
    if (!(ht->flags_ & HT_FLAG_CAN_WRITE)) {
        if (err) { *err = strdup("Hash table is read-only. Cannot insert."); }
        return -EACCES;
    }
    if (strlen(key) >= header_of(ht)->opts_.key_maxlen) {
        if (err) { *err = strdup("Key is too long."); }
        return -EINVAL;
    }
    /* Max load is 50% */
    if (cheader_of(ht)->cursize_ / 2 <= cheader_of(ht)->slots_used_) {
        if (!dht_reserve(ht, cheader_of(ht)->slots_used_ + 1, err)) return -ENOMEM;
    }
    uint64_t h = hash_key(key, ht->flags_ & HT_FLAG_HASH_2) % cheader_of(ht)->cursize_;
    while (1) {
        HashTableEntry et = entry_at(ht, h);
        if (entry_empty(et)) break;
        if (!strcmp(et.ht_key, key)) {
            return 0;
        }
        ++h;
        if (h == cheader_of(ht)->cursize_) {
            h = 0;
        }
    }
    set_table_at(ht, h, header_of(ht)->slots_used_ + 1);
    ++header_of(ht)->slots_used_;
    HashTableEntry et = entry_at(ht, h);

    strcpy((char*)et.ht_key, key);
    memcpy(et.ht_data, data, cheader_of(ht)->opts_.object_datalen);
    return 1;
}

int dht_update(HashTable* ht, const char* key, const void* data, char** err) {
    if (!(ht->flags_ & HT_FLAG_CAN_WRITE)) {
        if (err) { *err = strdup ("Hash table is read-only. Cannot update."); }
        return -EACCES;
    }
    void * data_ptr = dht_lookup (ht, key);
    if (data_ptr) {
        memcpy (data_ptr, data, header_of (ht)->opts_.object_datalen);
        return 1;
    }
    if (err) { *err = strdup ("Key was not found."); }
    return 0;
}

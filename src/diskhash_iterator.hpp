#ifndef DISKHASH_WRAPPERS_DISKHASH_ITERATOR_HPP
#define DISKHASH_WRAPPERS_DISKHASH_ITERATOR_HPP

#include <diskhash.hpp>

#include <cstring>
#include <iterator>
#include <utility>
#include <algorithm>
#include <memory>
#include <iostream>

namespace dht {

template <typename T>
struct DiskHash<T>::iterator {

    iterator (size_t index, DiskHash<T> & dht)
    :   dht_reference (dht),
        current_key (make_current_key (dht)),
        current_value (make_current_value (dht)),
        current (std::pair<std::string&, T&>{*current_key, *current_value}) {
        current_index = set_forward_index (index);
    }

    std::pair<std::string&, T&> & operator* () const {
        return current;
    }

    std::pair<std::string&, T&> * operator-> () {
        return &current;
    }

    DiskHash<T>::iterator & operator++ () {
        current_index = set_forward_index(++current_index);
        return *this;
    }

    bool operator== (DiskHash<T>::iterator const & other_iterator) {
        bool equal = (&dht_reference == &other_iterator.dht_reference)
            && (current_index == other_iterator.current_index)
            && ((current_key.get() != nullptr) == (other_iterator.current_key.get() != nullptr))
            && ((current_value.get() != nullptr) == (other_iterator.current_value.get() != nullptr))
            && ((current_key.get() != nullptr)
                    ? (current_key->compare(*other_iterator.current_key) == 0)
                    : true)
            && ((current_value.get() != nullptr)
                    ? (!std::memcmp(current_value.get(), other_iterator.current_value.get(), sizeof(T)))
                    : true);
        return equal;
    }

    bool operator!= (DiskHash<T>::iterator const & other_iterator) {
        return !(*this == other_iterator);
    }

private:
    DiskHash<T> & dht_reference;
    size_t current_index = std::numeric_limits<size_t>::max();
    std::unique_ptr<std::string> current_key;
    std::unique_ptr<T> current_value;
    std::pair<std::string&, T&> current;

    std::unique_ptr<std::string> make_current_key (DiskHash<T> & dht) {
        auto key = std::make_unique<std::string>();
        key->reserve(dht.key_size);
        return key;
    }

    std::unique_ptr<T> make_current_value (DiskHash<T> & dht) {
        auto value = std::make_unique<T>();
        return value;
    }

    size_t set_forward_index (size_t index) {
        char* key_ptr = (char*) current_key->c_str();
        auto status (dht_indexed_lookup(dht_reference.ht_, index, &key_ptr, current_value.get(), nullptr));
        if (status == -EINVAL) {
            current_key = nullptr;
            current_value = nullptr;
            return (std::numeric_limits<size_t>::max());
        }
        if (status == -EFAULT) {
            auto slots_used (dht_slots_used(dht_reference.ht_));
            if (index < slots_used) {
                return set_forward_index(++index);
            } else {
                current_key = nullptr;
                current_value = nullptr;
                return (std::numeric_limits<size_t>::max());
            }
        }
        if (status == 1) {
            return index;
        }
        // There is no other available return for dht_indexed_lookup ().
        assert (false);
    }
};

}

#endif //DISKHASH_WRAPPERS_DISKHASH_ITERATOR_HPP

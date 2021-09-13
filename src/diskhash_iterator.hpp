#ifndef DISKHASH_WRAPPERS_DISKHASH_ITERATOR_HPP
#define DISKHASH_WRAPPERS_DISKHASH_ITERATOR_HPP

#include <diskhash.hpp>

#include <cstring>
#include <iterator>
#include <utility>
#include <algorithm>
#include <memory>

namespace dht {

template <typename T>
struct DiskHash<T>::iterator {

    iterator () = default;

    iterator (DiskHash<T>::iterator const &) = delete;

    std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> make_current (DiskHash<T>::iterator & other_iterator) {
        auto key = std::make_shared<std::string>(other_iterator.current_index.first);
        auto value = std::make_shared<T>(other_iterator.current_index.second);
        auto pair = std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> (key, value);
        return pair;
    }

    std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> make_current (DiskHash<T> & dht) {
        auto key = std::make_shared<std::string>("", dht.key_size);
        auto value = std::make_shared<T>();
        auto pair = std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> (key, value);
        return pair;
    }

    iterator (DiskHash<T>::iterator && other_iterator)
    :   dht_reference (other_iterator),
        current (make_current (other_iterator.dht_reference)) {
        current_index = other_iterator.current_index;
        other_iterator.current_index = std::numeric_limits<size_t>::max();
    }

    iterator (size_t index, DiskHash<T> & dht)
    :   dht_reference (dht),
        current (make_current (dht)) {
        current_index = set_forward_index(index);
    }

    std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> & operator* () const {
        return current;
    }

    std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> * operator-> () {
        return &current;
    }

    DiskHash<T>::iterator & operator++ () {
        current_index = set_forward_index(++current_index);
        return *this;
    }

    bool operator== (DiskHash<T>::iterator const & other_iterator) {
        bool equal = true
             && (&dht_reference == &other_iterator.dht_reference)
             && (current_index == other_iterator.current_index)
             && (dht_reference.key_size == other_iterator.dht_reference.key_size)
             && ((current.first != nullptr) == (other_iterator.current.first != nullptr))
             && ((current.second != nullptr) == (other_iterator.current.second != nullptr))
             && (current.first != nullptr
                     ? (current.first->compare(*other_iterator.current.first))
                     : true)
             && (current.second != nullptr
                     ? (!memcmp(current.second.get(), other_iterator.current.second.get(), sizeof(T)))
                     : true);
        return equal;
    }

    bool operator!= (DiskHash<T>::iterator const & other_iterator) {
        return !(*this == other_iterator);
    }

private:
    DiskHash<T> & dht_reference;
    size_t current_index = std::numeric_limits<size_t>::max();
    std::pair<std::shared_ptr<std::string>, std::shared_ptr<T>> current;

    size_t set_forward_index (size_t index) {
        char* key_ptr = (char*) current.first->c_str();
        auto status (dht_indexed_lookup(dht_reference.ht_, index, &key_ptr, current.second.get(), nullptr));
        if (status == -EINVAL) {
            current.first = nullptr;
            current.second = nullptr;
            return (std::numeric_limits<size_t>::max());
        }
        if (status == -EFAULT) {
            auto slots_used (dht_slots_used(dht_reference.ht_));
            if (index < slots_used) {
                return set_forward_index(++index);
            } else {
                current.first = nullptr;
                current.second = nullptr;
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

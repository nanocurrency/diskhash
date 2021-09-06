#include <cassert>
#include <iostream>
#include <limits>
#include <vector>
#include <string>
#include <cstring>

#include <diskhash.hpp>
#include "helper_functions.hpp"

void cpp_slow_tests_remove_feature_works_lot_of_entries ();

int main (int argc, char ** argv)
{
	std::cout << "cpp_slow_tests_remove_feature_works_lot_of_entries ():" << std::endl;
	cpp_slow_tests_remove_feature_works_lot_of_entries ();

	delete_temp_db_path (get_temp_path ());
	return 0;
}

void cpp_slow_tests_remove_feature_works_lot_of_entries ()
{
	auto key_maxlen = static_cast<uint64_t> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<char[256]> (key_maxlen));

	char value[256];
	std::strncpy (value, random_string(256).c_str(), sizeof(value));

	std::vector<std::string> keys;
	size_t n = 1'000'000;
	keys.reserve(n);
	ht->reserve(n);
	assert (ht->capacity() >= n);
	std::cout << "Checking the insert feature..." << std::endl;
	for (size_t i = 0; i < n; i++) {
		auto key(random_string(key_maxlen));
		auto status (ht->insert(key.c_str(), value));
		if (!status) {
			i--;
			continue;
		}
		keys.push_back(key);
	}
	assert (ht->size() == n);
	assert (keys.size() == n);
	std::cout << "Checking the lookup feature..." << std::endl;
	for (auto key : keys) {
		auto ret_lookup = ht->lookup(key.c_str());
		if (!ret_lookup) {
			std::cout << "Failed key: " << key << std::endl;
		}
		assert (ret_lookup);
	}
	std::cout << "Checking the delete feature..." << std::endl;
	for (auto key : keys) {
		auto ret_delete = ht->remove(key.c_str());
		if (!ret_delete) {
			std::cout << "Failed key: " << key << std::endl;
		}
		assert (ret_delete);
	}
	assert (ht->size() == 0);
}

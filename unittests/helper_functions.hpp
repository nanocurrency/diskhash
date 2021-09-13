#ifndef __HELPER_FUNCTIONS_H__
#define __HELPER_FUNCTIONS_H__

#include <diskhash.hpp>

#include <filesystem>
#include <random>
#include <string>

bool db_exists (const std::filesystem::path & path);
bool db_exists (const char * path);
std::filesystem::path unique_path ();
std::filesystem::path get_temp_path ();
std::string get_temp_db_path ();
void delete_temp_db_path (std::filesystem::path temp_path);
std::string random_string (size_t size = 32);

template <typename T>
std::shared_ptr<dht::DiskHash<T>> get_shared_ptr_to_dht_db (int key_size = 32, dht::OpenMode open_mode = dht::DHOpenRW)
{
	const auto db_path = get_temp_db_path ();
	auto dht_db = std::make_shared<dht::DiskHash<T>> (db_path.c_str (), key_size, open_mode);
	return dht_db;
}

#endif // __HELPER_FUNCTIONS_H__

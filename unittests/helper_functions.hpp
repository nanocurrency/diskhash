#ifndef __HELPER_FUNCTIONS_H__
#define __HELPER_FUNCTIONS_H__

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

#endif // __HELPER_FUNCTIONS_H__

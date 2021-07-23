#include <helper_functions.hpp>

#include <filesystem>
#include <string>

bool db_exists (const std::filesystem::path & p)
{
	return std::filesystem::exists (p);
}

bool db_exists (const char* path)
{
    return db_exists(std::filesystem::path (path));
}

std::string get_uuid() {
    static std::random_device dev;
    static std::mt19937 rng(dev());

    std::uniform_int_distribution<int> dist(0, 15);

    const char *v = "0123456789abcdef";
    const bool dash[] = { 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0 };

    std::string res;
    for (int i = 0; i < 16; i++) {
        if (dash[i]) res += "-";
        res += v[dist(rng)];
        res += v[dist(rng)];
    }
    return res;
}

std::filesystem::path unique_path ()
{
	auto unique_path (get_temp_path () / get_uuid ());
	std::filesystem::create_directory (unique_path);
	return unique_path;
}

std::filesystem::path get_temp_path ()
{
	auto current_path (std::filesystem::current_path () / "temp_db");
	std::filesystem::create_directory (current_path);
	return current_path;
}

std::string get_temp_db_path ()
{
	auto db_path (unique_path () / "testdb.dht");
	return std::string (db_path.string ());
}

void delete_temp_db_path (std::filesystem::path temp_path)
{
	std::filesystem::remove_all (temp_path);
}

std::string random_string(size_t size)
{
	std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
	std::random_device rd;
	std::mt19937 generator(rd());
	std::shuffle(str.begin(), str.end(), generator);
	return str.substr(0, (size-1));    // assumes 32 < number of characters in str
}

#include <os_wrappers.h>

#include <assert.h>
#include <helper_functions.hpp>

void os_wrappers_dht_delete_file_works ();
void os_wrappers_dht_open_file_creates_file ();

int main (int argc, char ** argv)
{
	printf ("os_wrappers_dht_delete_file_works ():\n");
	os_wrappers_dht_delete_file_works ();

	printf ("os_wrappers_dht_open_file_creates_file ():\n");
	os_wrappers_dht_open_file_creates_file ();
}

void os_wrappers_dht_delete_file_works ()
{
	auto file_path = unique_path() / "test_file.dht";
	const char* file_path_str = file_path.c_str ();
	dht_open_file (file_path_str, O_RDWR | O_CREAT, false);
	assert (db_exists (file_path_str));
	dht_delete_file (file_path_str);
	assert (!db_exists (file_path_str));
}

void os_wrappers_dht_open_file_creates_file ()
{
	auto file_path = unique_path() / "test_file.dht";
	const char* file_path_str = file_path.c_str ();
	dht_file_t file_descriptor = dht_open_file (file_path_str, O_RDWR | O_CREAT, false);
	assert (file_descriptor > 0);
	assert (db_exists (file_path_str));
}

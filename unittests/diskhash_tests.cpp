#include <assert.h>
#include <diskhash.h>
#include <helper_functions.hpp>
#include <memory.h>
#include <os_wrappers.h>

void diskhash_creates_db_file_successfully ();
void diskhash_requires_o_creat_to_create_new_db ();
void diskhash_db_saved_correctly ();
void diskhash_db_saved_correctly_after_update ();
void diskhash_key_maxlen_equals_to_key_size_returns_error ();
void diskhash_inserts_successfully ();
void diskhash_updates_successfully ();
void diskhash_lookup_retrieves_null_for_inexistent_key ();
void diskhash_lookup_retrieves_inserted_value ();
void diskhash_check_capacity ();
void diskhash_reserves_capacity_equal_or_more_than_requested ();
void diskhash_returns_correct_size ();
void diskhash_load_to_memory_issues_error_for_writable_databases ();
void diskhash_load_to_memory_loads_on_readonly_db ();
void diskhash_load_to_memory_works ();
void diskhash_write_error_on_memory_loaded_db ();

#ifdef __cplusplus
using namespace std;
#endif

int main (int argc, char ** argv)
{
	printf ("diskhash_creates_db_file_successfully ():\n");
	diskhash_creates_db_file_successfully ();

	printf ("diskhash_requires_o_creat_to_create_new_db ():\n");
	diskhash_requires_o_creat_to_create_new_db ();

	printf ("diskhash_db_saved_correctly ():\n");
	diskhash_db_saved_correctly ();

	printf ("diskhash_db_saved_correctly_after_update ():\n");
	diskhash_db_saved_correctly_after_update ();

	printf ("diskhash_key_maxlen_equals_to_key_size_returns_error ():\n");
	diskhash_key_maxlen_equals_to_key_size_returns_error ();

	printf ("diskhash_inserts_successfully ():\n");
	diskhash_inserts_successfully ();

	printf ("diskhash_updates_successfully ():\n");
	diskhash_updates_successfully ();

	printf ("diskhash_lookup_retrieves_inserted_value ():\n");
	diskhash_lookup_retrieves_inserted_value ();

	printf ("diskhash_lookup_retrieves_null_for_inexistent_key ():\n");
	diskhash_lookup_retrieves_null_for_inexistent_key ();

	printf ("diskhash_check_capacity ():\n");
	diskhash_check_capacity ();

	printf ("diskhash_reserves_capacity_equal_or_more_than_requested ():\n");
	diskhash_reserves_capacity_equal_or_more_than_requested ();

	printf ("diskhash_returns_correct_size ():\n");
	diskhash_returns_correct_size ();

	printf ("diskhash_load_to_memory_issues_error_for_writable_databases ():\n");
	diskhash_load_to_memory_issues_error_for_writable_databases ();

	printf ("diskhash_load_to_memory_loads_on_readonly_db ():\n");
	diskhash_load_to_memory_loads_on_readonly_db ();

	printf ("diskhash_load_to_memory_works ():\n");
	diskhash_load_to_memory_works ();

	printf ("diskhash_write_error_on_memory_loaded_db ():\n");
	diskhash_write_error_on_memory_loaded_db ();

	return 0;
}

void diskhash_creates_db_file_successfully ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	HashTableOpts opts;
	opts.key_maxlen = 6;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	assert (ht);
	assert (db_exists (db_path));
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_requires_o_creat_to_create_new_db ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	HashTableOpts opts;
	opts.key_maxlen = 6;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	assert (!ht);
	assert (!strcmp ("open call failed.", err));

	free ((char *)db_path);
	free ((char *)err);
}

void diskhash_db_saved_correctly ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	int insert_val = 123456;
	dht_insert (ht, key, &insert_val, &err);
	dht_free (ht);

	int * read_value = NULL;
	ht = dht_open (db_path, opts, O_RDWR, &err);
	read_value = (int *)dht_lookup (ht, key);
	assert (*read_value == insert_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_db_saved_correctly_after_update ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	int insert_val = 123456;
	dht_insert (ht, key, &insert_val, &err);
	int update_val = 789;
	dht_update (ht, key, &update_val, &err);
	dht_free (ht);

	int * read_value = NULL;
	ht = dht_open (db_path, opts, O_RDWR, &err);
	read_value = (int *)dht_lookup (ht, key);
	assert (*read_value == update_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_key_maxlen_equals_to_key_size_returns_error ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key);
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123456;
	int insert_ret = dht_insert (ht, key, &insert_val, &err);
	assert (insert_ret == -EINVAL);
	assert (!strcmp ("Key is too long.", err));

	free ((char *)db_path);
	free ((char *)err);
	dht_free (ht);
}

void diskhash_inserts_successfully ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123456;
	int insert_ret = dht_insert (ht, key, &insert_val, &err);
	assert (insert_ret == 1);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_updates_successfully ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, key, &insert_val, &err);
	int update_val = 456;
	dht_update (ht, key, &update_val, &err);
	int* read_val = (int*) dht_lookup (ht, key);
	assert (update_val == *read_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_lookup_retrieves_inserted_value ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	int insert_val = 123456;
	dht_insert (ht, key, &insert_val, &err);

	int * read_val = (int *)dht_lookup (ht, key);
	assert (insert_val == *read_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_lookup_retrieves_null_for_inexistent_key ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int * read_val = (int *)dht_lookup (ht, key);
	assert (NULL == read_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_check_capacity ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	size_t capacity = dht_reserve (ht, 1, NULL);
	assert (1 <= capacity);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_reserves_capacity_equal_or_more_than_requested ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int capacity = dht_reserve (ht, 4, NULL);
	assert (4 <= capacity);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_returns_correct_size ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "key1", &insert_val, NULL);
	insert_val = 456;
	dht_insert (ht, "key2", &insert_val, NULL);

	int size = dht_size (ht);
	assert (2 == size);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_load_to_memory_issues_error_for_writable_databases ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int ret_load_to_mem = dht_load_to_memory (ht, NULL);
	assert (1 == ret_load_to_mem);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_load_to_memory_loads_on_readonly_db ()
{
	char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	dht_free (ht);
	ht = dht_open (db_path, opts, O_RDONLY, &err);

	int ret_load_to_mem = dht_load_to_memory (ht, NULL);
	assert (0 == ret_load_to_mem); // 0: successfully loaded
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_load_to_memory_works ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "key1", &insert_val, NULL);

	dht_free (ht);
	ht = dht_open (db_path, opts, O_RDONLY, &err);
	dht_load_to_memory (ht, NULL);

	int * read_val = (int *)dht_lookup (ht, "key1");
	assert (123 == *read_val);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_write_error_on_memory_loaded_db ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);
	dht_free (ht);

	ht = dht_open (db_path, opts, O_RDONLY, &err);
	dht_load_to_memory (ht, NULL);
	int insert_val = 456;
	int inserted_ok = dht_insert (ht, "key2", &insert_val, NULL);
	assert (-EACCES == inserted_ok);

	free ((char *)db_path);
	dht_free (ht);
}

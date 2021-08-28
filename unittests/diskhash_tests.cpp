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
void diskhash_inserts_increasing_capacity_works ();
void diskhash_updates_successfully ();
void diskhash_lookup_retrieves_null_for_inexistent_key ();
void diskhash_lookup_retrieves_inserted_value ();
void diskhash_check_capacity ();
void diskhash_reserves_capacity_equal_or_more_than_requested ();
void diskhash_returns_correct_size ();
void diskhash_returns_correct_capacity_after_insert ();
void diskhash_returns_correct_capacity_after_reserve ();
void diskhash_load_to_memory_issues_error_for_writable_databases ();
void diskhash_load_to_memory_loads_on_readonly_db ();
void diskhash_load_to_memory_works ();
void diskhash_write_error_on_memory_loaded_db ();
void diskhash_check_cursor_points_correctly_regardless_position ();
void diskhash_check_cursor_points_correctly_regardless_position_after_a_delete ();
void diskhash_check_cursor_points_correctly ();
void diskhash_deletes_correctly ();
void diskhash_deletes_three_entries_correctly ();
void diskhash_deletes_a_collision_correctly ();
void diskhash_deletes_more_than_one_collision_correctly ();
void diskhash_deletes_collision_with_filled_slot_correctly ();
void diskhash_reserve_is_not_affected_by_deleted_entries ();
void diskhash_deletes_first_slot_no_collision_correctly ();

#ifdef __cplusplus
using namespace std;
#endif

typedef struct dictionary {
	struct dictionary_entry {
		char key[30];
		int value;
	} entries[10];
	int size = 0;
	bool (*check_entry)(struct dictionary* dict, const char *k, int v);
	void (*append_entry)(struct dictionary* dict, const char* key, int value);
} dumb_dictionary_t;
bool check_entry_impl(struct dictionary* dict, const char* key, int value) {
	for(int i = 0; i < dict->size; i++) {
		if(!strcmp(dict->entries[i].key,key)) {
			return (dict->entries[i].value == value);
		}
	}
	return false;
}
void append_entry_impl(struct dictionary* dict, const char* key, int value) {
	strcpy(dict->entries[dict->size].key, key);
	dict->entries[dict->size].value = value;
	dict->size++;
}

int main (int argc, char ** argv)
{
	printf ("diskhash_check_cursor_points_correctly ():\n");
	diskhash_check_cursor_points_correctly ();

	printf ("diskhash_check_cursor_points_correctly_regardless_position ():\n");
	diskhash_check_cursor_points_correctly_regardless_position ();

	printf ("diskhash_check_cursor_points_correctly_regardless_position_after_a_delete ():\n");
	diskhash_check_cursor_points_correctly_regardless_position_after_a_delete ();

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

	printf ("diskhash_inserts_increasing_capacity_works ():\n");
	diskhash_inserts_increasing_capacity_works ();

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

	printf ("diskhash_returns_correct_capacity_after_insert():\n");
	diskhash_returns_correct_capacity_after_insert();

	printf ("diskhash_returns_correct_capacity_after_reserve():\n");
	diskhash_returns_correct_capacity_after_reserve();

	printf ("diskhash_load_to_memory_issues_error_for_writable_databases ():\n");
	diskhash_load_to_memory_issues_error_for_writable_databases ();

	printf ("diskhash_load_to_memory_loads_on_readonly_db ():\n");
	diskhash_load_to_memory_loads_on_readonly_db ();

	printf ("diskhash_load_to_memory_works ():\n");
	diskhash_load_to_memory_works ();

	printf ("diskhash_write_error_on_memory_loaded_db ():\n");
	diskhash_write_error_on_memory_loaded_db ();

	printf ("diskhash_deletes_correctly ():\n");
	diskhash_deletes_correctly ();

	printf ("diskhash_deletes_three_entries_correctly ():\n");
	diskhash_deletes_three_entries_correctly ();

	printf ("diskhash_deletes_a_collision_correctly():\n");
	diskhash_deletes_a_collision_correctly();

	printf ("diskhash_deletes_more_than_one_collision_correctly ():\n");
	diskhash_deletes_more_than_one_collision_correctly ();

	printf ("diskhash_deletes_collision_with_filled_slot_correctly ():\n");
	diskhash_deletes_collision_with_filled_slot_correctly ();

	printf ("diskhash_reserve_is_not_affected_by_deleted_entries ():\n");
	diskhash_reserve_is_not_affected_by_deleted_entries ();

	printf ("diskhash_deletes_first_slot_no_collision_correctly ():\n");
	diskhash_deletes_first_slot_no_collision_correctly ();

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

void diskhash_inserts_increasing_capacity_works ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

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
	test_dictionary.append_entry (&test_dictionary, "key1", insert_val);
	insert_val = 456;
	dht_insert (ht, "key2", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key2", insert_val);
	insert_val = 789;
	dht_insert (ht, "key0", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key0", insert_val);
	insert_val = 1;
	dht_insert (ht, "key4", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key4", insert_val);

	char* read_key = (char*) malloc(opts.key_maxlen);
	int* read_val;

	strcpy(read_key, "key1");
	read_val = (int*)dht_lookup (ht, read_key);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, *read_val));

	strcpy(read_key, "key2");
	read_val = (int*)dht_lookup (ht, read_key);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, *read_val));

	strcpy(read_key, "key0");
	read_val = (int*)dht_lookup (ht, read_key);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, *read_val));

	strcpy(read_key, "key4");
	read_val = (int*)dht_lookup (ht, read_key);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, *read_val));

	free ((char*) read_key);
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

void diskhash_returns_correct_capacity_after_insert ()
{

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	// Returns correct initial capacity
	assert (dht_capacity(ht) == 3);

	int insert_val = 123;
	dht_insert (ht, "key1", &insert_val, NULL);
	dht_insert (ht, "key2", &insert_val, NULL);
	dht_insert (ht, "key3", &insert_val, NULL);
	dht_insert (ht, "key4", &insert_val, NULL);

	assert (dht_capacity(ht) >= 4);

	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_returns_correct_capacity_after_reserve ()
{

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int ret_reserve = dht_reserve(ht, 5, &err);
	assert (ret_reserve);
	assert (dht_capacity(ht) >= 5);

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

// Kept this test because it may be useful for testing with delete feature.
void diskhash_check_cursor_points_correctly_regardless_position ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

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
	test_dictionary.append_entry (&test_dictionary, "key1", insert_val);
	insert_val = 456;
	dht_insert (ht, "key2", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key2", insert_val);
	insert_val = 789;
	dht_insert (ht, "key0", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key0", insert_val);

	char* read_key = (char*) malloc(opts.key_maxlen);
	int read_val;
	dht_indexed_lookup (ht, 0, &read_key, (void *)&read_val, &err);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, read_val));
	dht_indexed_lookup (ht, 1, &read_key, (void *)&read_val, &err);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, read_val));
	dht_indexed_lookup (ht, 2, &read_key, (void *)&read_val, &err);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, read_val));

	free ((char*) read_key);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_check_cursor_points_correctly_regardless_position_after_a_delete ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

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
	test_dictionary.append_entry (&test_dictionary, "key1", insert_val);
	insert_val = 456;
	dht_insert (ht, "key2", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key2", insert_val);
	insert_val = 789;
	dht_insert (ht, "key0", &insert_val, NULL);
	test_dictionary.append_entry (&test_dictionary, "key0", insert_val);

	dht_delete(ht, "key0", NULL);

	char* read_key = (char*) malloc(opts.key_maxlen);
	int read_val;
	dht_indexed_lookup (ht, 0, &read_key, (void *)&read_val, &err);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, read_val));
	dht_indexed_lookup (ht, 1, &read_key, (void *)&read_val, &err);
	assert (test_dictionary.check_entry(&test_dictionary, read_key, read_val));

	free ((char*) read_key);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_deletes_correctly ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	char* insert_key = strdup("key1");
	dht_insert (ht, insert_key, &insert_val, NULL);

	int ret_delete = dht_delete (ht, insert_key, &err);
	assert (ret_delete == 1);

	int* read_val = (int*)dht_lookup (ht, insert_key);
	assert (read_val == NULL);

	int ret_insert = dht_insert (ht, insert_key, &insert_val, NULL);
	assert (ret_insert == 1);

	free ((char *)insert_key);
	free ((char *)db_path);
	dht_free (ht);
}


void diskhash_deletes_three_entries_correctly ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "key", &insert_val, NULL);
	dht_insert(ht, "b", &insert_val, &err);
	dht_insert(ht, "x", &insert_val, &err);

//    Hash Table:
//    [ 0 ] = 3    [ x ]
//    [ 1 ] = 0
//    [ 2 ] = 0
//    [ 3 ] = 1    [ key ]
//    [ 4 ] = 2    [ b ]
//    [ 5 ] = 0
//    [ 6 ] = 0
//
//    Store Table:
//    [ 0 ] = [ zero ]
//    [ 1 ] = [ key: key, offset: 1 ]
//    [ 2 ] = [ key: b, offset: 1 ]
//    [ 3 ] = [ key: x, offset: 1 ]

	dht_delete(ht, "b", &err);
	int* ret_lookup = (int*)dht_lookup(ht, "b");
	assert (ret_lookup == NULL);

	dht_delete(ht, "key", &err);
	ret_lookup = (int*)dht_lookup(ht, "key");
	assert (ret_lookup == NULL);

	dht_delete(ht, "x", &err);
	ret_lookup = (int*)dht_lookup(ht, "x");
	assert (ret_lookup == NULL);

//    Dirty Stack:
//    [ 0 ] = 2
//    [ 1 ] = 1
//    [ 2 ] = 3

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_deletes_a_collision_correctly ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "ker", &insert_val, NULL);
	dht_insert (ht, "kex", &insert_val, NULL);
	dht_insert (ht, "key", &insert_val, NULL);

	// h("ker") == h("key") == 3
	// h("kex") == 2

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ ker ]
	//	[ 4 ] = 2    [ kex ]
	//	[ 5 ] = 3    [ key ]
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = [ zero ]
	//	[ 1 ] = [ key: ker, offset: 1 ]
	//	[ 2 ] = [ key: kex, offset: 1 ]
	//	[ 3 ] = [ key: key, offset: 3 ]

	int ret_delete = dht_delete(ht, "ker", &err);
	assert (ret_delete == 1);

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ key ]
	//	[ 4 ] = 2    [ kex ]
	//	[ 5 ] = 0
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = { zero }
	//	[ 1 ] = { key: key, offset: 1 }
	//	[ 2 ] = { key: kex, offset: 1 }
	//	[ 3 ] = { offset: 0 }

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_deletes_more_than_one_collision_correctly ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "ker", &insert_val, NULL);
	dht_insert (ht, "key", &insert_val, NULL);
	dht_insert (ht, "kez", &insert_val, NULL);

	// h("ker") == h("key") == h("kez")

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ ker ]
	//	[ 4 ] = 2    [ key ]
	//	[ 5 ] = 3    [ kez ]
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = [ zero ]
	//	[ 1 ] = [ key: ker, offset: 1 ]
	//	[ 2 ] = [ key: key, offset: 2 ]
	//	[ 3 ] = [ key: kez, offset: 3 ]

	int ret_delete = dht_delete(ht, "ker", &err);
	assert (ret_delete == 1);

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ key ]
	//	[ 4 ] = 2    [ kex ]
	//	[ 5 ] = 0
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = { zero }
	//	[ 1 ] = { key: key, offset: 1 }
	//	[ 2 ] = { key: kez, offset: 2 }
	//	[ 3 ] = { offset: 0 }

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_deletes_collision_with_filled_slot_correctly ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "key", &insert_val, NULL);
	dht_insert (ht, "kez", &insert_val, NULL);
	dht_insert (ht, "kex", &insert_val, NULL);

	// h("key") == h("kez") == 3
	// h("kex") == 4

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ key ]
	//	[ 4 ] = 2    [ kez ]
	//	[ 5 ] = 3    [ kex ]
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = [ zero ]
	//	[ 1 ] = [ key: ker, offset: 1 ]
	//	[ 2 ] = [ key: key, offset: 2 ]
	//	[ 3 ] = [ key: kez, offset: 2 ]

	int ret_delete = dht_delete(ht, "kex", &err);
	assert (ret_delete == 1);

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 1    [ key ]
	//	[ 4 ] = 2    [ kez ]
	//	[ 5 ] = 0
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = { zero }
	//	[ 1 ] = { key: key, offset: 1 }
	//	[ 2 ] = { key: kez, offset: 2 }
	//	[ 3 ] = { offset: 0 }

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_deletes_first_slot_no_collision_correctly ()
{
	dumb_dictionary_t test_dictionary = {0};
	test_dictionary.check_entry = check_entry_impl;
	test_dictionary.append_entry = append_entry_impl;

	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "ket", &insert_val, NULL);
	dht_insert (ht, "key", &insert_val, NULL);
	dht_insert (ht, "kez", &insert_val, NULL);

	// h("ket") == 2
	// h("key") == h("kez") == 3

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 1    [ ket ]
	//	[ 3 ] = 2    [ key ]
	//	[ 4 ] = 3    [ kez ]
	//	[ 5 ] = 0
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = [ zero ]
	//	[ 1 ] = [ key: ket, offset: 1 ]
	//	[ 2 ] = [ key: key, offset: 1 ]
	//	[ 3 ] = [ key: kez, offset: 2 ]

	int ret_delete = dht_delete(ht, "ket", &err);
	assert (ret_delete == 1);

	// Hash Table:
	//	[ 0 ] = 0
	//	[ 1 ] = 0
	//	[ 2 ] = 0
	//	[ 3 ] = 2    [ key ]
	//	[ 4 ] = 3    [ kez ]
	//	[ 5 ] = 0
	//	[ 6 ] = 0

	// Store Table:
	//	[ 0 ] = { zero }
	//	[ 1 ] = { offset: 1 }
	//	[ 2 ] = { key: key, offset: 1 }
	//	[ 3 ] = { key: kez, offset: 2 }

	//	DS {
	//		dirty slots = 1
	//		capacity = 3
	//
	//		[ 0 ] = 1
	//	}

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_reserve_is_not_affected_by_deleted_entries ()
{
	const char * db_path = strdup (get_temp_db_path ().c_str ());
	const char * key = "my_biggest_key";
	HashTableOpts opts;
	opts.key_maxlen = strlen (key) + 1;
	opts.object_datalen = sizeof (int);
	int flags = O_RDWR | O_CREAT;
	char * err = NULL;
	HashTable * ht = dht_open (db_path, opts, flags, &err);

	int insert_val = 123;
	dht_insert (ht, "KeyA", &insert_val, NULL);
	dht_insert(ht, "KeyB", &insert_val, &err);
	dht_delete(ht, "KeyB", &err);
	assert (dht_size(ht) == 1);

	dht_reserve(ht, 5, &err);
	assert (dht_size(ht) == 1);

	free ((char *)err);
	free ((char *)db_path);
	dht_free (ht);
}

void diskhash_check_cursor_points_correctly ()
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
	insert_val = 789;
	dht_insert (ht, "key0", &insert_val, NULL);

	char* read_key = (char*) malloc(opts.key_maxlen);
	int read_val;
	dht_indexed_lookup (ht, 0, &read_key, (void *)&read_val, &err);
	assert (read_val == 123);
	assert (!strcmp (read_key, "key1"));
	dht_indexed_lookup (ht, 1, &read_key, (void *)&read_val, &err);
	assert (read_val == 456);
	assert (!strcmp (read_key, "key2"));
	dht_indexed_lookup (ht, 2, &read_key, (void *)&read_val, &err);
	assert (read_val == 789);
	assert (!strcmp (read_key, "key0"));

	free ((char*) read_key);
	free ((char *)db_path);
	dht_free (ht);
}

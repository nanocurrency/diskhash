#include <cassert>
#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <string>

#include <diskhash.hpp>
#include <helper_functions.hpp>

void cpp_wrapper_slow_test ();
void cpp_wrapper_inserting_repeated_key_returns_false ();
void cpp_wrapper_successful_insert ();
void cpp_wrapper_filled_key_lookup_returns_value ();
void cpp_wrapper_empty_key_lookup_returns_null ();
void cpp_wrapper_is_member_with_existing_key_returns_true ();
void cpp_wrapper_is_member_with_unexisting_key_returns_false ();
void cpp_wrapper_db_creates_ok_with_DHOpenRW ();
void cpp_wrapper_db_disk_persistence_works ();
void cpp_wrapper_db_is_not_created_with_DHOpenRWNoCreate_and_returns_exception ();
void cpp_wrapper_move_constructor ();
void cpp_wrapper_clear_cleans_the_table ();

int main (int argc, char ** argv)
{
	std::cout << "cpp_wrapper_slow_test ():" << std::endl;
	cpp_wrapper_slow_test ();

	std::cout << "cpp_wrapper_inserting_repeated_key_returns_false ():" << std::endl;
	cpp_wrapper_inserting_repeated_key_returns_false ();

	std::cout << "cpp_wrapper_successful_insert ():" << std::endl;
	cpp_wrapper_successful_insert ();

	std::cout << "cpp_wrapper_filled_key_lookup_returns_value ():" << std::endl;
	cpp_wrapper_filled_key_lookup_returns_value ();

	std::cout << "cpp_wrapper_empty_key_lookup_returns_null ():" << std::endl;
	cpp_wrapper_empty_key_lookup_returns_null ();

	std::cout << "cpp_wrapper_is_member_with_existing_key_returns_true ():" << std::endl;
	cpp_wrapper_is_member_with_existing_key_returns_true ();

	std::cout << "cpp_wrapper_is_member_with_unexisting_key_returns_false ():" << std::endl;
	cpp_wrapper_is_member_with_unexisting_key_returns_false ();

	std::cout << "cpp_wrapper_db_creates_ok_with_DHOpenRW ():" << std::endl;
	cpp_wrapper_db_creates_ok_with_DHOpenRW ();

	std::cout << "cpp_wrapper_db_disk_persistence_works ():" << std::endl;
	cpp_wrapper_db_disk_persistence_works ();

	std::cout << "cpp_wrapper_db_is_not_created_with_DHOpenRWNoCreate_and_returns_exception ():" << std::endl;
	cpp_wrapper_db_is_not_created_with_DHOpenRWNoCreate_and_returns_exception ();

	std::cout << "void cpp_wrapper_move_constructor ():" << std::endl;
	cpp_wrapper_move_constructor ();

	std::cout << "cpp_wrapper_clear_cleans_the_table ():" << std::endl;
	cpp_wrapper_clear_cleans_the_table ();

	delete_temp_db_path (get_temp_path ());
	return 0;
}

template <
typename T,
typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
std::shared_ptr<dht::DiskHash<T>> get_shared_ptr_to_dht_db (int key_size = 32, dht::OpenMode open_mode = dht::DHOpenRW)
{
	const auto db_path = get_temp_db_path ();
	auto dht_db = std::make_shared<dht::DiskHash<T>> (db_path.c_str (), key_size, open_mode);
	return dht_db;
}

void cpp_wrapper_slow_test ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	uint64_t index = 0;
	std::list<std::string> keys;
	for (auto i = 0; i < 3; ++i)
	{
		auto key (random_string (key_maxlen));
		keys.emplace_back (key);
		ht->insert (key.c_str (), index++);
	}

	for (auto & key : keys)
	{
		auto value = ht->lookup (key.c_str ());
		if (!value)
		{
			std::cerr << "Value not found for key: " << key << std::endl;
		}
	}
}

void cpp_wrapper_successful_insert ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto status (ht->insert (key.c_str (), 1245));
	assert (status);
}

void cpp_wrapper_inserting_repeated_key_returns_false ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	ht->insert (key.c_str (), 1245);
	auto status (ht->insert (key.c_str (), 3232));
	assert (!status);
}

void cpp_wrapper_empty_key_lookup_returns_null ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto value = ht->lookup (key.c_str ());
	assert (value == nullptr);
}

void cpp_wrapper_filled_key_lookup_returns_value ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);
	auto lookup_value_ptr = ht->lookup (key.c_str ());
	assert (insert_value == *lookup_value_ptr);
}

void cpp_wrapper_is_member_with_existing_key_returns_true ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);

	auto found (ht->is_member (key.c_str ()));
	assert (found);
}

void cpp_wrapper_is_member_with_unexisting_key_returns_false ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto another_key (random_string (key_maxlen));
	assert (key != another_key);

	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);

	auto found (ht->is_member (another_key.c_str ()));
	assert (!found);
}

void cpp_wrapper_db_creates_ok_with_DHOpenRW ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());

	const auto db_path = get_temp_db_path ();
	auto dht_db = dht::DiskHash<uint64_t> (db_path.c_str (), key_maxlen, dht::DHOpenRW);

	auto exists (db_exists (db_path));
	assert (exists);
}

void cpp_wrapper_db_disk_persistence_works ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto key (random_string (key_maxlen - 1));
	auto insert_value = uint64_t (12345);
	const auto db_path = get_temp_db_path ();
	{
		auto ht = std::make_shared<dht::DiskHash<uint64_t>> (db_path.c_str (), key_maxlen, dht::DHOpenRW);
		ht->insert (key.c_str (), insert_value);
	}
	{
		auto ht = std::make_shared<dht::DiskHash<uint64_t>> (db_path.c_str (), key_maxlen, dht::DHOpenRWNoCreate);
		auto read_value = ht->lookup (key.c_str ());
		assert (insert_value == *read_value);
	}
}

void cpp_wrapper_db_is_not_created_with_DHOpenRWNoCreate_and_returns_exception ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	const auto db_path = get_temp_db_path ();

	try
	{
		dht::DiskHash<uint64_t> dht_db (db_path.c_str (), key_maxlen, dht::DHOpenRWNoCreate);
	}
	catch (std::runtime_error ex)
	{
		return;
	}
	assert (false);
}

void cpp_wrapper_move_constructor ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	ht->insert ("abc", 123);
	auto another_ht (std::move (*ht));
	assert (another_ht.is_member ("abc"));
}

void cpp_wrapper_clear_cleans_the_table ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);
	assert (ht->size () > 0);

	ht->clear();
	assert (ht->size () == 0);
}

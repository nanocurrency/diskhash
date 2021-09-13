#include <diskhash.hpp>
#include <diskhash_iterator.hpp>
#include <helper_functions.hpp>

#include <cassert>
#include <iostream>
#include <limits>
#include <list>
#include <unordered_map>
#include <memory>
#include <string>
#include <cstring>

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
void cpp_wrapper_reserve_works_for_increasing_table_allocation ();
void cpp_wrapper_reserve_just_returns_when_passing_same_capacity ();
void cpp_wrapper_reserve_just_returns_when_passing_lower_capacity ();
void cpp_wrapper_remove_feature_works_any_key ();
void cpp_wrapper_remove_nonexistent_key_returns_false ();
void cpp_wrapper_remove_with_invalid_key_throws_exception ();
void cpp_wrappper_iterator_begin_ok ();
void cpp_wrappper_iterator_different_than_operator_works ();
void cpp_wrappper_iterator_equals_to_operator_works ();
void cpp_wrappper_iterator_increment_operator_works ();

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

	std::cout << "cpp_wrapper_reserve_works_for_increasing_table_allocation ():" << std::endl;
	cpp_wrapper_reserve_works_for_increasing_table_allocation ();

	std::cout << "cpp_wrapper_reserve_just_returns_when_passing_same_capacity ():" << std::endl;
	cpp_wrapper_reserve_just_returns_when_passing_same_capacity ();

	std::cout << "cpp_wrapper_reserve_just_returns_when_passing_lower_capacity ():" << std::endl;
	cpp_wrapper_reserve_just_returns_when_passing_lower_capacity ();

	std::cout << "cpp_wrapper_remove_feature_works_any_key ():" << std::endl;
	cpp_wrapper_remove_feature_works_any_key();

	std::cout << "cpp_wrapper_remove_nonexistent_key_returns_false ():" << std::endl;
	cpp_wrapper_remove_nonexistent_key_returns_false();

	std::cout << "cpp_wrapper_remove_with_invalid_key_throws_exception ():" << std::endl;
	cpp_wrapper_remove_with_invalid_key_throws_exception ();

	std::cout << "cpp_wrappper_iterator_different_than_operator_works ():" << std::endl;
	cpp_wrappper_iterator_different_than_operator_works ();

	std::cout << "cpp_wrappper_iterator_equals_to_operator_works ():" << std::endl;
	cpp_wrappper_iterator_equals_to_operator_works ();

	std::cout << "cpp_wrappper_iterator_increment_operator_works ():" << std::endl;
	cpp_wrappper_iterator_increment_operator_works ();

	std::cout << "cpp_wrappper_iterator_begin_ok ():" << std::endl;
	cpp_wrappper_iterator_begin_ok ();

	delete_temp_db_path (get_temp_path ());
	return 0;
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

void cpp_wrapper_reserve_works_for_increasing_table_allocation ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto new_allocation = ht->capacity() + 1;
	ht->reserve(new_allocation);
	assert (ht->capacity() >= new_allocation);
}

void cpp_wrapper_reserve_just_returns_when_passing_same_capacity ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto same_capacity = ht->capacity();
	ht->reserve(same_capacity);
	assert (ht->capacity() == same_capacity);
}

void cpp_wrapper_reserve_just_returns_when_passing_lower_capacity ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto current_capacity = ht->capacity();
	auto lower_capacity = current_capacity - 1;
	ht->reserve(lower_capacity);
	assert (ht->capacity() == current_capacity);
}

void cpp_wrapper_remove_feature_works_any_key ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	ht->insert (key.c_str (), 1245);

	auto ret_delete = ht->remove(key.c_str());
	if (!ret_delete) {
		std::cout << "Failed key: " << key << std::endl;
	}
	assert (ret_delete);
	assert (ht->size() == 0);
}

void cpp_wrapper_remove_nonexistent_key_returns_false ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto ret_delete = ht->remove(key.c_str());
	assert (!ret_delete);
}

void cpp_wrapper_remove_with_invalid_key_throws_exception ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	try {
		ht->remove(nullptr);
	} catch (const std::invalid_argument &e) {
		assert (!strcmp("The informed key is an invalid NULL pointer.", e.what()));
		return;
	}
	assert (false);
}

void cpp_wrappper_iterator_different_than_operator_works ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key1 (random_string (key_maxlen));
	ht->insert (key1.c_str (), 12);

	auto it = ht->begin();
	auto end_it = ht->end();
	assert (it != end_it);
}

void cpp_wrappper_iterator_equals_to_operator_works ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key1 (random_string (key_maxlen));
	ht->insert (key1.c_str (), 12);

	auto it1 (ht->begin());
	auto it2 (ht->begin());
	assert (it1 == it2);
}

void cpp_wrappper_iterator_increment_operator_works ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key1 (random_string (key_maxlen));
	ht->insert (key1.c_str (), 12);

	auto it (ht->begin());
	auto key (it->first);
	++it;
	assert (it == ht->end());
}

void cpp_wrappper_iterator_begin_ok ()
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	std::unordered_map<std::string, int> check_map;
	auto key1 (random_string (key_maxlen));
	auto key2 (random_string (key_maxlen));
	ht->insert (key1.c_str (), 12);
	check_map.emplace(key1, 12);
	ht->insert (key2.c_str (), 34);
	check_map.emplace(key2, 34);

	auto number_of_elements (ht->size());
	auto it = ht->begin();
	auto end_it = ht->end();
	auto counter(0u);

	for (; it != end_it; ++it, ++counter);
	assert (number_of_elements == counter);
}

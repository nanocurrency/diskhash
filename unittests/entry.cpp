#include <gtest/gtest.h>

#include <helper_functions.hpp>

int main (int argc, char ** argv)
{
	testing::InitGoogleTest (&argc, argv);
	auto res = RUN_ALL_TESTS ();
	delete_temp_db_path (get_temp_path ());
	return res;
}

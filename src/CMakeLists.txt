cmake_minimum_required(VERSION 3.4)

project(diskhash-core)

#add_custom_target(all_targets DEPENDS libdht diskhashtools disktest)

add_library(libdht SHARED diskhash.c diskhash.h rtable.h)

add_executable(diskhashtools diskhashtools.cpp diskhash.hpp)
target_link_libraries(diskhashtools libdht)
set_property(TARGET diskhashtools PROPERTY CMAKE_POSITION_INDEPENDENT_CODE)

add_executable(disktest disktest.c diskhash.h)
target_link_libraries(disktest libdht)
set_property(TARGET disktest PROPERTY CMAKE_POSITION_INDEPENDENT_CODE)

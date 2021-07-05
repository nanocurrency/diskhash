cmake_minimum_required(VERSION 3.4)

project(diskhash-wrappers)

#add_custom_target(all_targets DEPENDS c python haskell)

add_subdirectory(gtest)
#set_target_properties(gtest PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
#        "${CMAKE_SOURCE_DIR}/gtest/googletest/include")


include_directories(src)
include_directories(unittests)

add_subdirectory(src)

add_subdirectory(unittests)

#add_custom_target(python DEPENDS build_python)

#add_custom_target(haskell COMMAND stack build)

#add_custom_target(check DEPENDS check_python check_haskell)

#add_custom_target(check_haskell COMMAND stack test)

#add_custom_target(build_python COMMAND python setup.py build_ext --inplace)

#add_custom_target(
#  check_python
#  DEPENDS build_python
#  COMMAND pytest)

#add_custom_target(install_python python setup.py install)

#option(WITH_ASAN "build with ASAN" OFF)
#if(WITH_ASAN)
#    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
#    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
#endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pie")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIC")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -fPIC")
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -g")

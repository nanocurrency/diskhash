cmake_minimum_required(VERSION 3.4)

set(PROJECT_NAME diskhash-wrappers)
set(DEPLOY_NAME ${PROJECT_NAME})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
                      ${CMAKE_CURRENT_SOURCE_DIR}/cmake-modules/)
message("CMake Module Path: " ${CMAKE_MODULE_PATH})
project(${PROJECT_NAME})

option(WITH_ASAN ON)
option(DISKHASH_TESTS ON)

set(CXX_FLAGS
    CMAKE_CXX_FLAGS
    CMAKE_CXX_FLAGS_DEBUG
    CMAKE_CXX_FLAGS_MINSIZEREL
    CMAKE_CXX_FLAGS_RELEASE
    CMAKE_CXX_FLAGS_RELWITHDEBINFO
)

include_directories(${CMAKE_SOURCE_DIR}/src)
add_library(diskhash STATIC src/os_wrappers.c src/diskhash.c)

file(GLOB HDR_FILES ${CMAKE_SOURCE_DIR}/src/*.h*)
set_target_properties(diskhash PROPERTIES PUBLIC_HEADER "${HDR_FILES}")

install(TARGETS diskhash
  EXPORT diskhashTargets
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include
  PUBLIC_HEADER DESTINATION include/diskhash
)

install(EXPORT diskhashTargets
  FILE diskhashConfig.cmake
  DESTINATION lib/cmake/diskhash
)

if(DISKHASH_TESTS)
  include_directories(${CMAKE_SOURCE_DIR}/unittests)

  add_executable(cpp_wrapper_tests unittests/helper_functions.cpp
                                   unittests/cpp_wrapper_tests.cpp)
  target_link_libraries(cpp_wrapper_tests diskhash)

  add_executable(cpp_slow_tests unittests/helper_functions.cpp
          unittests/cpp_slow_tests.cpp)
  target_link_libraries(cpp_slow_tests diskhash)

  add_executable(diskhash_tests unittests/helper_functions.cpp
                                unittests/diskhash_tests.cpp)
  target_link_libraries(diskhash_tests diskhash)

  add_executable(os_wrappers_tests unittests/helper_functions.cpp
                                   unittests/os_wrappers_tests.cpp)
  target_link_libraries(os_wrappers_tests diskhash)

  add_executable(disktest src/disktest.c)

  target_link_libraries(disktest diskhash)
endif()

if(NOT MSVC)
  option(WITH_ASAN "build with ASAN" OFF)
  if(WITH_ASAN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  endif()

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -g")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -g")
endif()

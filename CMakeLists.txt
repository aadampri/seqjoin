cmake_minimum_required(VERSION 3.20)
project(seqjoin LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Header-only library
add_library(seqjoin INTERFACE)
target_include_directories(seqjoin INTERFACE include)

# Tests
enable_testing()

add_executable(test_basic tests/test_basic.cpp)
target_link_libraries(test_basic PRIVATE seqjoin)
target_compile_options(test_basic PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_basic COMMAND test_basic)

add_executable(test_concurrent tests/test_concurrent.cpp)
target_link_libraries(test_concurrent PRIVATE seqjoin)
target_compile_options(test_concurrent PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(test_concurrent PRIVATE pthread)
add_test(NAME test_concurrent COMMAND test_concurrent)

add_executable(test_phase4 tests/test_phase4.cpp)
target_link_libraries(test_phase4 PRIVATE seqjoin)
target_compile_options(test_phase4 PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_phase4 COMMAND test_phase4)

add_executable(test_integration tests/test_integration.cpp)
target_link_libraries(test_integration PRIVATE seqjoin)
target_compile_options(test_integration PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_integration COMMAND test_integration)

add_executable(test_retain_by_key tests/test_retain_by_key.cpp)
target_link_libraries(test_retain_by_key PRIVATE seqjoin)
target_compile_options(test_retain_by_key PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_retain_by_key COMMAND test_retain_by_key)

add_executable(test_policies tests/test_policies.cpp)
target_link_libraries(test_policies PRIVATE seqjoin)
target_compile_options(test_policies PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME test_policies COMMAND test_policies)

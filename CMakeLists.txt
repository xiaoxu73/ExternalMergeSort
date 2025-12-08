cmake_minimum_required(VERSION 3.10)
project(ExternalMergeSort LANGUAGES CXX)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译器选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -pthread")

# 指定可执行文件输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 使用FetchContent下载Google Test
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.14.0
)

FetchContent_MakeAvailable(googletest)

# 包含目录
include_directories(include)

# 创建测试可执行文件
add_executable(merge_sort_tests
    test/merge_sort_test.cpp
    src/external_merge_sort.cpp
    src/thread_pool.cpp
)

# 链接Google Test和pthread
target_link_libraries(merge_sort_tests GTest::gtest_main GTest::gtest pthread)

# 添加测试
include(GoogleTest)
gtest_discover_tests(merge_sort_tests)
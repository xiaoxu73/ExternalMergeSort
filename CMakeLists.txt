cmake_minimum_required(VERSION 3.10)
project(ExternalMergeSort LANGUAGES CXX)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 根据构建类型设置不同的编译选项
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -pthread")
    message(STATUS "Building in Debug mode")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG -Wall -pthread")
    message(STATUS "Building in Release mode")
else()
    # 默认使用调试配置
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -Wall -pthread")
    message(STATUS "Building in default Debug mode")
endif()

# 指定可执行文件输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)

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
CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -pthread
INCLUDES = -Iinclude
LDFLAGS = -pthread

# 目标文件
OBJECTS = src/main.o src/thread_pool.o src/external_merge_sort.o
DATA_GEN_OBJECTS = src/generate_data.o
TEST_OBJECTS = test/merge_sort_test.o src/thread_pool.o src/external_merge_sort.o

# 可执行文件
TEST_TARGET = merge_sort_tests

.PHONY: all clean test

$(TEST_TARGET): $(TEST_OBJECTS)
	$(CXX) $(TEST_OBJECTS) -o $(TEST_TARGET) $(LDFLAGS) -lgtest -lgtest_main

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: $(TEST_TARGET)
	./$(TEST_TARGET)

clean:
	rm -f $(OBJECTS) $(DATA_GEN_OBJECTS) $(TEST_OBJECTS) $(TEST_TARGET)

# 依赖关系
src/thread_pool.o: src/thread_pool.cpp include/thread_pool.h
src/external_merge_sort.o: src/external_merge_sort.cpp include/external_merge_sort.h include/thread_pool.h
test/merge_sort_test.o: test/merge_sort_test.cpp include/external_merge_sort.h include/thread_pool.h

# 如果没有系统级的gtest，提供一种本地编译gtest的方法
GTEST_DIR ?= /usr/src/googletest/googletest
LOCAL_GTEST = $(GTEST_DIR)/libgtest.a $(GTEST_DIR)/libgtest_main.a

local-gtest:
	@if [ ! -f "$(GTEST_DIR)/libgtest.a" ]; then \
		echo "Building Google Test locally"; \
		cd $(GTEST_DIR); \
		$(CXX) -isystem . -isystem ../include -I. -I../include -pthread -c src/gtest-all.cc; \
		ar rv libgtest.a gtest-all.o; \
		$(CXX) -isystem . -isystem ../include -I. -I../include -pthread -c src/gtest_main.cc; \
		ar rv libgtest_main.a gtest_main.o; \
	fi
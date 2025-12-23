#include <gtest/gtest.h>
#include <iostream>
#include <fstream>
#include <random>
#include <filesystem>
#include <chrono>
#include <sys/resource.h>
#include "../include/external_merge_sort.h"
#include "../src/generate_data.cpp"

namespace fs = std::filesystem;

class ExternalMergeSortTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建测试目录
        test_dir = "test_data_" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count());
        // test_dir = "test_data_128GB";
        output_file = "sorted_output.dat";
        fs::create_directories(test_dir);
    }

    void TearDown() override {
        // 清理测试数据
        if (fs::exists(test_dir)) {
            fs::remove_all(test_dir);
        }
        if (fs::exists(output_file)) {
            fs::remove(output_file);
        }
        // 清理可能产生的临时文件
        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".tmp") {
                fs::remove(entry.path());
            }
        }
    }

    // 生成测试数据文件
    void generate_test_file(const std::string& filename, size_t count, bool random = true) {
        std::ofstream file(filename, std::ios::binary);
        ASSERT_TRUE(file.is_open());

        if (random) {
            std::random_device rd;
            std::mt19937_64 gen(rd());
            std::uniform_int_distribution<int64_t> dis(INT64_MIN, INT64_MAX);

            for (size_t i = 0; i < count; ++i) {
                int64_t value = dis(gen);
                file.write(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        } else {
            // 生成有序数据（用于测试正确性）
            for (size_t i = 0; i < count; ++i) {
                int64_t value = static_cast<int64_t>(count - i - 1);
                file.write(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        }
        file.close();
    }

    // 生成多个测试文件
    void generate_multiple_test_files(size_t file_count, size_t elements_per_file) {
        fs::create_directory(test_dir);
        for (size_t i = 0; i < file_count; ++i) {
            std::string filename = test_dir + "/data_" + std::to_string(i) + ".dat";
            generate_test_file(filename, elements_per_file);
        }
    }

    // 验证输出文件是否已排序
    bool is_file_sorted(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) return false;

        int64_t prev, current;
        file.read(reinterpret_cast<char*>(&prev), sizeof(prev));
        if (file.eof()) return true; // 空文件或只有一个元素

        while (file.read(reinterpret_cast<char*>(&current), sizeof(current))) {
            if (prev > current) {
                file.close();
                return false;
            }
            prev = current;
        }
        file.close();
        return true;
    }

    // 计算文件中的元素数量
    size_t count_file_elements(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file.is_open()) return 0;

        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        return file_size / sizeof(int64_t);
    }

    // 获取当前内存使用情况（以MB为单位）
    double get_memory_usage_mb() {
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        return usage.ru_maxrss / 1024.0; // ru_maxrss以KB为单位
    }

    std::string test_dir;
    std::string output_file;
};

// 测试边界条件：空文件
TEST_F(ExternalMergeSortTest, EmptyFiles) {
    const size_t FILE_COUNT = 3;

    std::cout << "\n=== 测试空文件处理 ===" << std::endl;
    std::cout << "空文件数量: " << FILE_COUNT << std::endl;

    // 生成空文件
    for (size_t i = 0; i < FILE_COUNT; ++i) {
        std::string filename = test_dir + "/empty_" + std::to_string(i) + ".dat";
        generate_test_file(filename, 0);
    }

    ExternalMergeSorter sorter(test_dir, output_file, 32 * 1024 * 1024);
    sorter.sort();

    // 输出文件应该存在且为空（或接近空）
    ASSERT_TRUE(fs::exists(output_file));
    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(0, output_elements);
}

// 测试边界条件：单个文件
TEST_F(ExternalMergeSortTest, SingleFile) {
    const size_t ELEMENTS = 5000;

    std::cout << "\n=== 测试单文件处理 ===" << std::endl;

    std::string filename = test_dir + "/single.dat";
    generate_test_file(filename, ELEMENTS);

    ExternalMergeSorter sorter(test_dir, output_file, 8 * 1024 * 1024); // 8MB内存限制
    sorter.sort();

    ASSERT_TRUE(fs::exists(output_file));
    EXPECT_TRUE(is_file_sorted(output_file));

    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(ELEMENTS, output_elements);
}

// 测试已经排序的数据
TEST_F(ExternalMergeSortTest, PreSortedData) {
    const size_t FILE_COUNT = 5;
    const size_t ELEMENTS_PER_FILE = 2000;

    std::cout << "\n=== 测试预排序数据 ===" << std::endl;

    // 生成已排序的测试文件（降序，让排序有意义）
    for (size_t i = 0; i < FILE_COUNT; ++i) {
        std::string filename = test_dir + "/sorted_" + std::to_string(i) + ".dat";
        generate_test_file(filename, ELEMENTS_PER_FILE, false); // 生成有序数据
    }

    ExternalMergeSorter sorter(test_dir, output_file, 16 * 1024 * 1024);
    sorter.sort();

    ASSERT_TRUE(fs::exists(output_file));
    EXPECT_TRUE(is_file_sorted(output_file));

    size_t total_input_elements = FILE_COUNT * ELEMENTS_PER_FILE;
    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(total_input_elements, output_elements);
}

// 测试包含大量重复数据的情况
TEST_F(ExternalMergeSortTest, DuplicateElements) {
    const size_t FILE_COUNT = 10;
    const size_t ELEMENTS_PER_FILE = 10000;
    const int64_t DUPLICATE_VALUE = 42;

    std::cout << "\n=== 测试重复数据处理 ===" << std::endl;

    // 生成包含大量重复元素的文件
    for (size_t i = 0; i < FILE_COUNT; ++i) {
        std::string filename = test_dir + "/duplicate_" + std::to_string(i) + ".dat";
        std::ofstream file(filename, std::ios::binary);
        ASSERT_TRUE(file.is_open());

        for (size_t j = 0; j < ELEMENTS_PER_FILE; ++j) {
            int64_t value = (j % 2 == 0) ? DUPLICATE_VALUE : static_cast<int64_t>(ELEMENTS_PER_FILE - j);
            file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        file.close();
    }

    ExternalMergeSorter sorter(test_dir, output_file, 8 * 1024 * 1024);
    sorter.sort();

    ASSERT_TRUE(fs::exists(output_file));
    EXPECT_TRUE(is_file_sorted(output_file));

    size_t total_input_elements = FILE_COUNT * ELEMENTS_PER_FILE;
    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(total_input_elements, output_elements);
}

// 测试极小内存限制情况
TEST_F(ExternalMergeSortTest, MinimalMemoryLimit) {
    const size_t ELEMENTS = 5000;

    std::cout << "\n=== 测试极小内存限制 ===" << std::endl;
    std::cout << "内存限制: 1KB" << std::endl;

    std::string filename = test_dir + "/small_memory.dat";
    generate_test_file(filename, ELEMENTS);

    // 设置极小的内存限制（只有1KB）
    ExternalMergeSorter sorter(test_dir, output_file, 1024);
    sorter.sort();

    ASSERT_TRUE(fs::exists(output_file));
    EXPECT_TRUE(is_file_sorted(output_file));

    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(ELEMENTS, output_elements);
}

// 测试超大数据集
TEST_F(ExternalMergeSortTest, LargeRandomDataset) {
    std::cout << "\n=== 测试使用generate_test_data函数创建的超大数据集 ===" << std::endl;
    
    // 使用generate_test_data函数生成大数据集
    // 生成约128GB的数据，分布在100000个文件中
    const size_t FILE_COUNT = 100000;
    const size_t TOTAL_GB = 128; // 128GB的数据集
    
    // 生成测试数据
    generate_test_data(test_dir, FILE_COUNT, TOTAL_GB);
    
    // 计算总元素数
    size_t total_elements = 0;
    for (const auto& entry : fs::directory_iterator(test_dir)) {
        if (entry.is_regular_file()) {
            total_elements += count_file_elements(entry.path());
        }
    }
    
    std::cout << "总元素数: " << total_elements << std::endl;
    std::cout << "生成数据大小: " << (total_elements * sizeof(int64_t)) / (1024*1024) << " MB" << std::endl;

    double mem_before = get_memory_usage_mb();
    auto start_time = std::chrono::high_resolution_clock::now();

    // 使用64MB内存限制进行排序
    ExternalMergeSorter sorter(test_dir, output_file, 64 * 1024 * 1024);
    sorter.sort();

    auto end_time = std::chrono::high_resolution_clock::now();
    double mem_after = get_memory_usage_mb();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "排序总耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "内存使用: " << mem_before << " MB -> " << mem_after << " MB" << std::endl;

    // 验证输出文件存在且已排序
    ASSERT_TRUE(fs::exists(output_file));
    EXPECT_TRUE(is_file_sorted(output_file));

    // 验证元素总数一致
    size_t output_elements = count_file_elements(output_file);
    EXPECT_EQ(total_elements, output_elements);
}
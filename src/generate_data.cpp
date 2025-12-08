#include <iostream>
#include <fstream>
#include <random>
#include <filesystem>
#include <vector>
#include <string>
#include <chrono>

namespace fs = std::filesystem;

void generate_test_data(const std::string& dir, size_t num_files, size_t total_gb) {
    // 创建目录
    fs::create_directories(dir);
    
    // 计算总数据量(以int64_t为单位)
    size_t total_elements = (total_gb * 1024ULL * 1024ULL * 1024ULL) / sizeof(int64_t);
    size_t avg_elements_per_file = total_elements / num_files;
    
    std::cout << "生成测试数据:" << std::endl;
    std::cout << "- 总计约 " << total_gb << " GB 数据" << std::endl;
    std::cout << "- 分散在 " << num_files << " 个文件中" << std::endl;
    std::cout << "- 平均每文件约 " << avg_elements_per_file << " 个元素" << std::endl;
    
    // 初始化随机数生成器
    std::random_device rd;  // 获取硬件随机数做种子
    std::mt19937_64 gen(rd());   // 使用Mersenne Twister 64位随机数生成器

    // 创建均匀分布用于生成64位随机整数
    std::uniform_int_distribution<int64_t> dis(INT64_MIN, INT64_MAX);
    // 生成泊松分布用于创建不同大小的文件
    std::poisson_distribution<size_t> size_dist(avg_elements_per_file);
    
    for (size_t i = 0; i < num_files; ++i) {
        // 为每个文件生成随机数量的元素
        size_t elements_in_file = size_dist(gen);  // 从泊松分布中采样，生成文件大小
        if (elements_in_file < 1000) elements_in_file = 1000; // 限制文件中至少1000个元素
        // 二进制格式
        std::string filename = dir + "/data_" + std::to_string(i) + ".dat";
        std::ofstream file(filename, std::ios::binary);
        
        if (!file.is_open()) {
            std::cerr << "无法创建文件: " << filename << std::endl;
            continue;
        }
        
        // 写入随机数据
        for (size_t j = 0; j < elements_in_file; ++j) {
            int64_t value = dis(gen);  // 从均匀分布中采样，生成随机整数
            file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        
        file.close();
        
        // 每生成约 10% 的文件，输出一次进度
        if ((i + 1) % (num_files / 10 + 1) == 0) {
            std::cout << "已生成 " << (i + 1) << " / " << num_files << " 个文件" << std::endl;
        }
    }
    
    std::cout << "数据生成完成!" << std::endl;
}

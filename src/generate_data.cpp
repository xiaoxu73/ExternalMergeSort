#include <iostream>
#include <fstream>
#include <random>
#include <filesystem>
#include <vector>
#include <string>
#include <chrono>
#include <cmath>
#include <algorithm>

namespace fs = std::filesystem;

void generate_test_data(const std::string& dir, size_t num_files, size_t total_gb) {
    // 创建目录
    if (!fs::exists(dir)) {
        fs::create_directories(dir);
    }
    
    // 计算总数据量(以int64_t为单位)
    size_t total_elements = (total_gb * 1024ULL * 1024ULL * 1024ULL) / sizeof(int64_t);
    size_t avg_elements_per_file = total_elements / num_files;
    
    std::cout << "生成测试数据:" << std::endl;
    std::cout << "- 总计约 " << total_gb << " GB 数据" << std::endl;
    std::cout << "- 分散在 " << num_files << " 个文件中" << std::endl;
    
    // 初始化随机数生成器
    std::random_device rd;  // 获取硬件随机数做种子
    std::mt19937_64 gen(rd());   // 使用Mersenne Twister 64位随机数生成器

    // 创建均匀分布用于生成64位随机整数
    std::uniform_int_distribution<int64_t> dis(INT64_MIN, INT64_MAX);
    
    // 使用对数正态分布生成文件大小，这样可以产生跨越多个数量级的文件大小
    // 对于对数正态分布，如果ln(X) ~ N(μ,σ^2)，则X的均值为exp(μ+σ^2/2)
    // 我们希望X的均值约为avg_elements_per_file，所以需要适当地选择μ和σ
    
    // 设置σ值控制分布的跨度，值越大跨度越大
    double sigma = 2.0;
    // 根据σ值计算μ值，使得均值等于avg_elements_per_file
    double mu = std::log(static_cast<double>(avg_elements_per_file)) - sigma * sigma / 2;
    
    std::lognormal_distribution<double> size_dist(mu, sigma);
    
    // 用于追踪最大和最小文件大小
    size_t max_elements = 0;
    size_t min_elements = std::numeric_limits<size_t>::max();
    size_t total_generated_elements = 0;
    
    for (size_t i = 0; i < num_files; ++i) {
        // 为每个文件生成随机数量的元素
        size_t elements_in_file = static_cast<size_t>(size_dist(gen));
        
        // 限制文件中至少1000个元素，防止过小的文件
        if (elements_in_file < 1000) elements_in_file = 1000;
        
        // 更新统计数据
        max_elements = std::max(max_elements, elements_in_file);
        min_elements = std::min(min_elements, elements_in_file);
        total_generated_elements += elements_in_file;
        
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
    std::cout << "统计信息:" << std::endl;
    std::cout << "- 最小文件大小: " << min_elements << " 个元素 (" << (min_elements * sizeof(int64_t) / (1024.0 * 1024.0)) << " MB)" << std::endl;
    std::cout << "- 最大文件大小: " << max_elements << " 个元素 (" << (max_elements * sizeof(int64_t) / (1024.0 * 1024.0)) << " MB)" << std::endl;
    std::cout << "- 总共生成元素数: " << total_generated_elements << std::endl;
    std::cout << "- 总共生成数据量: " << (total_generated_elements * sizeof(int64_t) / (1024.0 * 1024.0 * 1024.0)) << " GB" << std::endl;
}
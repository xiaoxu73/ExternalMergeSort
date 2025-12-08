#include "external_merge_sort.h"
#include <iostream>
#include <fstream>
#include <algorithm>
#include <filesystem>
#include <queue>
#include <cstring>

namespace fs = std::filesystem;

ExternalMergeSorter::ExternalMergeSorter(const std::string& input_dir,
                                         const std::string& output_file,
                                         size_t memory_limit)
    : input_dir_(input_dir), 
      output_file_(output_file),
      memory_limit_(memory_limit) {
    // 线程数量等于CPU核心数
    num_threads_ = std::thread::hardware_concurrency();
    if (num_threads_ == 0) num_threads_ = 32; // 默认值
    
    // 创建线程池
    thread_pool_ = std::make_unique<ThreadPool>(num_threads_);
    std::cout << "线程池创建成功，线程数: " << num_threads_ << std::endl;
}

void ExternalMergeSorter::sort() {
    std::cout << "开始分割和预排序阶段..." << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    auto chunks = splitAndPresort();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "分割和预排序完成，耗时: " << duration.count() << "ms" << std::endl;
    
    std::cout << "开始多路归并阶段..." << std::endl;
    start_time = std::chrono::high_resolution_clock::now();

    mergeChunks(chunks);

    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "多路归并完成，耗时: " << duration.count() << "ms" << std::endl;
    
    // 清理临时文件
    for (const auto& chunk : chunks) {
        fs::remove(chunk.temp_file);
    }
    
    std::cout << "排序完成，结果保存至: " << output_file_ << std::endl;
}

std::vector<ExternalMergeSorter::ChunkInfo> ExternalMergeSorter::splitAndPresort() {
    auto files = getAllFiles(input_dir_);
    std::vector<std::future<std::vector<ChunkInfo>>> futures;
    
    // 并行处理所有文件
    for (const auto& file : files) {
        auto future = thread_pool_->submit(&ExternalMergeSorter::processFile, this, file);
        // 收集所有future
        futures.push_back(std::move(future));
    }
    
    // 收集所有结果
    std::vector<ChunkInfo> chunks;
    for (auto& future : futures) {
        auto chunks_from_one_file = future.get(); // 得到 vector<ChunkInfo>
        // 拼接到总列表
        chunks.insert(chunks.end(),
                          std::make_move_iterator(chunks_from_one_file.begin()),
                          std::make_move_iterator(chunks_from_one_file.end()));
    }
    
    return chunks;
}

std::vector<ExternalMergeSorter::ChunkInfo> ExternalMergeSorter::processFile(const std::string& filepath) {
    // 计算可以加载到内存中的数据量（预留一些空间）
    size_t max_elements = memory_limit_ / (num_threads_ * sizeof(int64_t));
    std::vector<int64_t> buffer;
    buffer.reserve(max_elements);
    
    std::ifstream input(filepath, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("无法打开文件: " + filepath);
    }

    std::vector<ChunkInfo> chunk_infos;
    bool finished = false;
    size_t round = 0;
    while (!finished) {
        buffer.clear();

        // 创建临时文件名
        std::string chunk_filename = filepath + ".sorted" + std::to_string(round++);

        // 打开临时文件
        std::ofstream output(chunk_filename, std::ios::binary);
        if (!output.is_open()) {
            throw std::runtime_error("无法创建临时文件: " + chunk_filename);
        }
    
        ChunkInfo info;
        info.temp_file = chunk_filename;
        info.data_count = 0;
        
        // 读取一批数据到缓冲区
        for (size_t i = 0; i < max_elements; ++i) {
            int64_t value;
            if (input.read(reinterpret_cast<char*>(&value), sizeof(value))) {
                buffer.push_back(value);
                info.data_count++;
            } else {
                finished = true;
                break;
            }
        }
        
        // 排序缓冲区内的数据
        std::sort(buffer.begin(), buffer.end());
        
        for (const auto& value : buffer) {
            output.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        chunk_infos.push_back(info);
    }
    
    input.close();
    return chunk_infos;
}

void ExternalMergeSorter::mergeChunks(const std::vector<ChunkInfo>& chunks) {
    // 简化版本：两阶段合并
    // 1. 如果块数不多，直接进行k路归并
    // 2. 如果块数很多，则分层合并
    
    if (chunks.empty()) {
        return;
    }
    
    // 对于简单实现，我们直接进行多路归并
    std::vector<std::string> filenames;
    
    for (const auto& chunk : chunks) {
        filenames.push_back(chunk.temp_file);
    }
    
    mergeFiles(filenames, output_file_);
}

void ExternalMergeSorter::mergeFiles(const std::vector<std::string>& files, const std::string& output_file) {
    // 打开所有输入文件
    std::vector<std::ifstream> inputs(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        inputs[i].open(files[i], std::ios::binary);
        if (!inputs[i].is_open()) {
            throw std::runtime_error("无法打开文件: " + files[i]);
        }
    }
    
    // 打开输出文件
    std::ofstream output(output_file, std::ios::binary);
    if (!output.is_open()) {
        throw std::runtime_error("无法创建输出文件: " + output_file);
    }
    
    // 使用最小堆进行k路归并
    struct Element {
        int64_t value;
        size_t stream_index;
        
        bool operator>(const Element& other) const {
            return value > other.value;
        }
    };
    
    std::priority_queue<Element, std::vector<Element>, std::greater<Element>> min_heap;
    
    // 初始化堆，从每个文件读取第一个元素
    for (size_t i = 0; i < inputs.size(); ++i) {
        int64_t value;
        if (inputs[i].read(reinterpret_cast<char*>(&value), sizeof(value))) {
            min_heap.push({value, i});
        }
    }
    
    // 归并过程
    while (!min_heap.empty()) {
        Element elem = min_heap.top();
        min_heap.pop();
        
        // 写入当前最小元素到输出文件
        output.write(reinterpret_cast<const char*>(&elem.value), sizeof(elem.value));
        
        // 从相同文件流中读取下一个元素
        int64_t next_value;
        if (inputs[elem.stream_index].read(reinterpret_cast<char*>(&next_value), sizeof(next_value))) {
            min_heap.push({next_value, elem.stream_index});
        }
    }
    
    // 关闭所有文件
    for (auto& input : inputs) {
        input.close();
    }
    output.close();
}

// 获取目录下所有普通文件的路径
std::vector<std::string> ExternalMergeSorter::getAllFiles(const std::string& dir) const {
    std::vector<std::string> files;
    
    try {
        for (const auto& entry : fs::recursive_directory_iterator(dir)) {
            if (entry.is_regular_file()) {
                files.push_back(entry.path().string());
            }
        }
    } catch (const fs::filesystem_error& ex) {
        std::cerr << "遍历目录时出错: " << ex.what() << std::endl;
    }
    
    return files;
}
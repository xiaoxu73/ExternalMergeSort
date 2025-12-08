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
    if (files.empty()) {
        return {};
    }

    if (num_threads_ == 0) {
        // 单线程处理
        std::vector<ChunkInfo> chunks;
        for (const auto& file : files) {
            chunks.push_back(processFile(file));
        }
        return chunks;
    }

    // 按批次提交任务，避免一次性创建过多future对象，并实现更好的负载均衡
    const size_t batch_size = std::max(static_cast<size_t>(1), files.size() / (num_threads_ * 2));
    
    std::vector<ChunkInfo> chunks;
    for (size_t i = 0; i < files.size(); i += batch_size) {
        size_t end = std::min(i + batch_size, files.size());
        std::vector<std::future<ChunkInfo>> batch_futures;
        
        // 提交一批任务
        for (size_t j = i; j < end; ++j) {
            auto future = thread_pool_->submit(&ExternalMergeSorter::processFile, this, files[j]);
            batch_futures.push_back(std::move(future));
        }
        
        // 等待这批任务完成并收集结果
        for (auto& future : batch_futures) {
            chunks.push_back(future.get());
        }
    }
    
    return chunks;
}

ExternalMergeSorter::ChunkInfo ExternalMergeSorter::processFile(const std::string& filepath) {
    // 使用一半的内存限制，为其他操作留出空间
    size_t max_elements = memory_limit_ / (num_threads_ * sizeof(int64_t));
    std::vector<int64_t> buffer;
    buffer.reserve(max_elements);
    
    std::ifstream input(filepath, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("无法打开文件: " + filepath);
    }
    
    // 创建主临时文件名
    std::string temp_filename = filepath + ".sorted";
    
    ChunkInfo info;
    info.temp_file = temp_filename;
    info.data_count = 0;
    
    bool finished = false;
    size_t chunk_index = 0;
    
    // 存储所有中间chunk文件
    std::vector<std::string> chunk_files;
    
    while (!finished) {
        buffer.clear();
        
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
        
        // 将排序后的数据写入临时chunk文件
        std::string chunk_filename = temp_filename + ".chunk" + std::to_string(chunk_index++);
        chunk_files.push_back(chunk_filename);
        
        std::ofstream output(chunk_filename, std::ios::binary);
        if (!output.is_open()) {
            throw std::runtime_error("无法创建临时文件: " + chunk_filename);
        }
        
        for (const auto& value : buffer) {
            output.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        output.close();
    }
    
    input.close();
    
    // 合并所有生成的chunk文件
    if (chunk_files.size() == 1) {
        // 只有一个chunk，直接重命名为最终的临时文件
        fs::rename(chunk_files[0], temp_filename);
    } 
    else if (chunk_files.size() > 1) {
        // 多个chunk，需要进行内部归并
        mergeFiles(chunk_files, temp_filename);
        // 删除中间chunk文件
        for (const auto& chunk_file : chunk_files) {
            fs::remove(chunk_file);
        }
    }
    
    return info;
}

void ExternalMergeSorter::mergeChunks(const std::vector<ChunkInfo>& chunks) {
    if (chunks.empty()) {
        return;
    }
    
    // 如果只有一个文件，直接复制即可
    if (chunks.size() == 1) {
        fs::copy_file(chunks[0].temp_file, output_file_, fs::copy_options::overwrite_existing);
        return;
    }
    
    // 如果chunks数量较少，直接单线程多路归并
    if (chunks.size() <= 128) {
        std::vector<std::string> filenames;
        for (const auto& chunk : chunks) {
            filenames.push_back(chunk.temp_file);
        }
        mergeFiles(filenames, output_file_);
        return;
    }
    
    // 如果chunks数量很多，采用多线程分层并归策略
    std::vector<std::string> current_files;
    for (const auto& chunk : chunks) {
        current_files.push_back(chunk.temp_file);
    }
    
    // 循环合并直到只剩一个文件
    size_t round = 0;   // 合并轮数
    while (current_files.size() > 1) {
        std::vector<std::string> next_round_files; // 下一轮的文件列表
        const size_t merge_factor = 128; // 每轮最多合并128个文件
        
        // 使用线程池并行处理多个合并任务
        std::vector<std::future<void>> merge_futures;
        std::vector<std::vector<std::string>> files_groups;
        std::vector<std::string> output_files;
        
        for (size_t i = 0; i < current_files.size(); i += merge_factor) {
            size_t end = std::min(i + merge_factor, current_files.size());
            std::vector<std::string> files_to_merge(current_files.begin() + i, current_files.begin() + end);
            
            if (files_to_merge.size() == 1) {
                // 单个文件无需合并
                next_round_files.push_back(files_to_merge[0]);
            } else {
                // 准备并行合并任务
                std::string intermediate_file = output_file_ + ".intermediate" + "_r" + std::to_string(round) + "_g" + std::to_string(i);
                output_files.push_back(intermediate_file);
                files_groups.push_back(std::move(files_to_merge));
            }
        }
        
        // 提交并行任务
        for (size_t i = 0; i < files_groups.size(); ++i) {
            auto future = thread_pool_->submit([this, files_group = std::move(files_groups[i]) , output_file = std::move(output_files[i])]() {
                this->mergeFiles(files_group, output_file);
            });
            merge_futures.push_back(std::move(future));
        }
        
        // 等待所有并行任务完成
        for (auto& future : merge_futures) {
            future.wait();
        }
        
        // 收集结果并清理源文件
        for (size_t i = 0; i < files_groups.size(); ++i) {
            next_round_files.push_back(output_files[i]);
            // 删除已合并的源文件以节省空间
            for (const auto& file : files_groups[i]) {
                fs::remove(file);
            }
        }
        
        current_files = std::move(next_round_files);
        round++;
    }
    
    // 最终的文件即为输出文件
    if (!current_files.empty()) {
        fs::rename(current_files[0], output_file_);
    }
}

void ExternalMergeSorter::mergeFiles(const std::vector<std::string>& files, const std::string& output_file) {
    if (files.empty()) {
        return;
    }
    
    if (files.size() == 1) {
        // 单个文件直接复制
        fs::copy_file(files[0], output_file, fs::copy_options::overwrite_existing);
        return;
    }
    
    // 打开所有输入文件
    std::vector<std::ifstream> inputs(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        inputs[i].open(files[i], std::ios::binary);
        if (!inputs[i].is_open()) {
            // 关闭之前成功打开的所有文件
            for (size_t j = 0; j < i; ++j) {
                inputs[j].close();
            }
            throw std::runtime_error("无法打开文件: " + files[i]);
        }
    }
    
    // 打开输出文件
    std::ofstream output(output_file, std::ios::binary);
    if (!output.is_open()) {
        // 关闭所有已打开的输入文件
        for (auto& input : inputs) {
            input.close();
        }
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
#ifndef EXTERNAL_MERGE_SORT_H
#define EXTERNAL_MERGE_SORT_H

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include "thread_pool.h"

class ExternalMergeSorter {
public:
    ExternalMergeSorter(const std::string& input_dir, 
                       const std::string& output_file,
                       size_t memory_limit = 64 * 1024 * 1024); // 默认64MB
    
    void sort();

private:
    struct ChunkInfo {
        std::string temp_file;
        size_t data_count;
    };
    
    // 第一阶段：分割和预排序
    std::vector<ChunkInfo> splitAndPresort();
    
    // 处理单个文件
    std::vector<ExternalMergeSorter::ChunkInfo> processFile(const std::string& filepath);
    
    // 第二阶段：多路归并
    void mergeChunks(const std::vector<ChunkInfo>& chunks);
    
    // 辅助方法
    std::vector<std::string> getAllFiles(const std::string& dir) const;
    void mergeFiles(const std::vector<std::string>& files, const std::string& output_file);

    std::string input_dir_;
    std::string output_file_;
    size_t memory_limit_;
    std::unique_ptr<ThreadPool> thread_pool_;
    size_t num_threads_;
};

#endif // EXTERNAL_MERGE_SORT_H
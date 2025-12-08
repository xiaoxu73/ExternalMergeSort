#include "thread_pool.h"
#include <stdexcept>

ThreadPool::ThreadPool(size_t num_threads) : stop_(false) {
    // 创建工作线程，每个线程运行一个循环，不断从任务队列中取任务执行
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                
                {
                    // 使用作用域锁保护任务队列，退出作用域时自动解锁
                    std::unique_lock<std::mutex> lock(this->queue_mutex_); 

                    // 条件变量，线程阻塞，直到任务队列非空或者线程池停止，阻塞时释放锁，唤醒时重新获取锁
                    this->condition_.wait(lock, [this] { 
                        return this->stop_ || !this->tasks_.empty(); 
                    });

                    // 线程池停止且任务队列为空，退出线程
                    if (this->stop_ && this->tasks_.empty()) {
                        return;
                    }
                    
                    task = std::move(this->tasks_.front());   // 移动语义避免不必要的拷贝开销
                    this->tasks_.pop();   // 从队列中弹出任务
                }
                
                task();   // 执行任务
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    stop_ = true;   // 停止所有工作线程
    condition_.notify_all();   // 唤醒所有等待线程
    
    // 等待所有工作线程结束
    for (std::thread &worker : workers_) {
        worker.join();
    }
}
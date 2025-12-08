#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <atomic>
#include <memory>

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    // 删除拷贝构造函数和赋值运算符
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 提交任务到线程池，模版函数
    template<typename F, typename... Args>
    auto submit(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>;

private:
    // 工作线程容器
    std::vector<std::thread> workers_;
    
    // 任务队列
    std::queue<std::function<void()>> tasks_;
    
    // 同步原语
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    
    // 停止标志
    std::atomic<bool> stop_;
};

// 实现模板函数
template<typename F, typename... Args>
auto ThreadPool::submit(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>>
{
    using return_type = std::invoke_result_t<F, Args...>;

    // 包装任务，完美转移参数，减少拷贝
    auto task = std::make_shared<std::packaged_task<return_type()>>(
    [f = std::forward<F>(f), args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        return std::apply(f, std::move(args));
    }
    );

    // 创建一个 future 用于获取任务结果
    std::future<return_type> res = task->get_future();

    // 将任务添加到任务队列
    {
        std::unique_lock<std::mutex> lock(this->queue_mutex_);  // 加锁保护任务队列，退出作用域时自动解锁
        
        // 防止在停止后提交任务
        if (this->stop_) {
            throw std::runtime_error("submit on stopped ThreadPool");
        }
        
        this->tasks_.emplace([task]() { (*task)(); });
    }
    
    // 唤醒一个等待线程
    this->condition_.notify_one();
    return res;
}

#endif // THREAD_POOL_H
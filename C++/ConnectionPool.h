#pragma once
#include <condition_variable>
#include <chrono>

// 调用数据库API执行写入或查询，如果返回网络连接错误则继续尝试调用，
// 每次尝试调用API内部会做重连，直到成功
// GoE_C_ERRNO_ERROR   0xFFFF8000  // C语言errno错误的起始值
// 针对快照订阅、标签点订阅、API信息订阅，在开始订阅到关闭订阅之间，不要归还连接句柄，要一直占有，关闭订阅之后才可以归还
// 轮询快照、定期入库数据，可以在调用API之前获取连接，返回之后归还连接，从而提供连接的复用率

// 连接池
class connection_pool
{
public:
	struct connection_info
	{
		std::string ip;
		int port;
		std::string user;
		std::string password;
	};

public:
	connection_pool(connection_info &info, int pool_size)
		: conninfo(info)
		, pool_size_(pool_size)
	{
		//初始化，设置API连接参数
		setup();
	}

	// 析构之前，尽量把连接都归还到连接池，析构时可以主动关闭连接，对于未归还的连接，
	// 由操作系统在进程结束时统一回收资源
	~connection_pool()
	{
		clearup();
	}

	int pool_size_ = 0;
	int handle_count_ = 0;
	std::condition_variable condition_;

	int get_handle(); 
	void release_handle(int handle);

protected:
	void setup();
	void clearup();

private:
	connection_info conninfo;
	std::mutex login_mutex;
	std::mutex handle_mutex_;
	queue<int> handle_queue_;
};


inline void connection_pool::setup()
{
	//自动重连
	go_set_option(GOLDEN_API_AUTO_RECONN, 1);
	//连接超时5秒
	go_set_option(GOLDEN_API_CONN_TIMEOUT, 5);
}

inline int connection_pool::get_handle()
{

	if (handle_count_ < pool_size_)
	{
		bool login_success = false;
		int handle = 0, priv = 0;
		{
			std::unique_lock<std::mutex> lock(login_mutex);
			login_success = (GoE_OK == go_connect(conninfo.host.c_str(), conninfo.port, &handle) && GoE_OK == go_login(handle, conninfo.user.c_str(), conninfo.password.c_str(), &priv));
		}
		if (!login_success)
		{
			if (handle) { go_disconnect(handle); handle = 0; }
			throw std::runtime_error("connect golden database failed.");
			return -1;
		}
		else
		{
			handle_count_++;
			return handle;
		}
	}
	else {
		std::unique_lock<std::mutex> lock(handle_mutex_);
		while (handle_queue_.empty())
			condition_.wait(lock);
		int handle = handle_queue_.front();
		handle_queue_.pop();
		retrun handle;
	}
}

inline void connection_pool::release_handle(int handle) {
	std::unique_lock<std::mutex> lock(handle_mutex_);
	handle_queue_.push(handle);
	condition_.notify_one();
}

inline void connection_pool::clearup()
{
	auto func_destory_connections = [&](int &handle) { if (handle) { go_disconnect(handle); handle = 0; }};
	for_each(handle_queue_.begin(), handle_queue_.end(), func_destory_connections);
}

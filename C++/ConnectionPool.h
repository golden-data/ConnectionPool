#pragma once
#include <condition_variable>
#include <chrono>

// �������ݿ�APIִ��д����ѯ����������������Ӵ�����������Ե��ã�
// ÿ�γ��Ե���API�ڲ�����������ֱ���ɹ�
// GoE_C_ERRNO_ERROR   0xFFFF8000  // C����errno�������ʼֵ
// ��Կ��ն��ġ���ǩ�㶩�ġ�API��Ϣ���ģ��ڿ�ʼ���ĵ��رն���֮�䣬��Ҫ�黹���Ӿ����Ҫһֱռ�У��رն���֮��ſ��Թ黹
// ��ѯ���ա�����������ݣ������ڵ���API֮ǰ��ȡ���ӣ�����֮��黹���ӣ��Ӷ��ṩ���ӵĸ�����

// ���ӳ�
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
		//��ʼ��������API���Ӳ���
		setup();
	}

	// ����֮ǰ�����������Ӷ��黹�����ӳأ�����ʱ���������ر����ӣ�����δ�黹�����ӣ�
	// �ɲ���ϵͳ�ڽ��̽���ʱͳһ������Դ
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
	//�Զ�����
	go_set_option(GOLDEN_API_AUTO_RECONN, 1);
	//���ӳ�ʱ5��
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

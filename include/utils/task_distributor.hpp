#ifndef TASK_DISTRIBUTOR_HPP
#define TASK_DISTRIBUTOR_HPP

#include <future>
#include <thread>

namespace utils {
	template<typename TNext, typename TTask, typename TSave, typename... TArgs>
	void distribute_tasks(TNext const & next, TTask const & task, TSave const & save, const TArgs & ... args) {
		typedef std::result_of_t< decltype(next)&(TArgs...) > R;
		[[maybe_unused]] typedef std::result_of< decltype(task)&(R, TArgs...) > S;

		std::mutex queue_mutex;
		std::vector<auto> queue;

		std::atomic_size_t remaining { std::thread::hardware_concurrency() };

		while (const auto in = next(args...)) {
			std::thread([&] (const R value) {
				task(*value);
				++remaining;
				remaining.notify_one();
			}, in).detach();

			if (--remaining == 0) {
				remaining.wait(0);
			}
		}
	}
}

#endif //TASK_DISTRIBUTOR_HPP

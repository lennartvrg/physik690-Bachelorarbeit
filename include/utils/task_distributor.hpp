#ifndef TASK_DISTRIBUTOR_HPP
#define TASK_DISTRIBUTOR_HPP

#include <future>
#include <thread>
#include <queue>
#include <cassert>

namespace utils {
	template<typename TNext, typename TTask, typename... TArgs>
	void save_queue(const std::function<void(TNext, TTask, TArgs...)> & save, std::queue<std::tuple<TNext, TTask>> & queue, std::mutex & queue_mutex, const TArgs & ... args) {
		const std::scoped_lock lock { queue_mutex };
		while (!queue.empty()) {
			const auto [chunk, result] = queue.front();
			queue.pop();
			save(chunk, result, args...);
		}
	}

	template<typename TNext, typename TTask, typename... TArgs>
	void distribute_tasks(const std::function<std::optional<TNext>(TArgs...)> & next, const std::function<TTask(TNext)> & task, const std::function<void(TNext, TTask, TArgs...)> & save, const TArgs & ... args) {
		// The queue holds the task results
		std::mutex queue_mutex;
		std::queue<std::tuple<TNext, TTask>> queue {};

		// The maximum number of workers is equal to the hardware concurrency
		std::atomic_size_t workers { 0 };
		const auto max_workers = std::thread::hardware_concurrency();

		// As long as there are new tasks, we want to delegate the task to a new worker
		while (const std::optional<TNext> in = next(args...)) {
			workers.fetch_add(1, std::memory_order_seq_cst);

			// Starting to process the task on a new thread
			std::thread([&] (const TNext value) {
				const auto result = task(value);

				// Add the task result to the queue
				const std::scoped_lock lock { queue_mutex };
				queue.push(std::make_tuple(value, result));

				// Indicate that the thread is finished
				workers.fetch_sub(1, std::memory_order_seq_cst);
				workers.notify_one();
			}, *in).detach();

			// Wait until one worker has finished and save the queue
			while (workers== max_workers) {
				workers.wait(max_workers);
			}
			save_queue(save, queue, queue_mutex, args...);
		}

		// Wait until all workers are finished and drain the remaining queue
		for (std::size_t value; (value = workers.load(std::memory_order_seq_cst)) != 0;) {
			workers.wait(value);
		}
		save_queue(save, queue, queue_mutex, args...);
	}
}

#endif //TASK_DISTRIBUTOR_HPP

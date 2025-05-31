#ifndef TASK_HPP
#define TASK_HPP

#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <queue>
#include <optional>

#include "storage/storage.hpp"

namespace tasks {
	template<typename TStorage, typename TTask, typename TResult>
	requires std::is_base_of_v<Storage, TStorage> class Task {
	public:
		template<typename ... Args>
		explicit Task(const Config & config, Args && ... args) : config(config), storage(std::make_shared<TStorage>(std::forward<Args>(args)...)) {
			std::cout << "Preparing task" << std::endl;
			storage->prepare_simulation(config);
		}

		virtual ~Task() = default;

		void execute() {
			// The queue holds the task results
			std::mutex queue_mutex;
			std::queue<std::tuple<TTask, TResult>> queue {};

			// The maximum number of workers is equal to the hardware concurrency
			std::atomic_size_t workers { 0 };
			const auto max_workers = std::thread::hardware_concurrency();

			// As long as there are new tasks, we want to delegate the task to a new worker
			while (const std::optional<TTask> in = next_task()) {
				workers.fetch_add(1, std::memory_order_seq_cst);

				// Starting to process the task on a new thread
				std::thread([&] (const TTask value) {
					const auto result = execute_task(value);

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
				save_queue(queue, queue_mutex);
			};

			// Wait until all workers are finished and drain the remaining queue
			for (std::size_t value; (value = workers.load(std::memory_order_seq_cst)) != 0;) {
				workers.wait(value);
			}
			save_queue(queue, queue_mutex);
		}

	protected:
		virtual std::optional<TTask> next_task() = 0;

		virtual TResult execute_task(const TTask & task) = 0;

		virtual void save_task(const TTask & task, const TResult & result) = 0;

		const Config config;

		std::shared_ptr<TStorage> storage;

	private:
		void save_queue(std::queue<std::tuple<TTask, TResult>> & queue, std::mutex & queue_mutex) {
			const std::scoped_lock lock { queue_mutex };
			while (!queue.empty()) {
				const auto [task, result] = queue.front();
				queue.pop();
				save_task(task, result);
			}
		}
	};
}

#endif //TASK_HPP

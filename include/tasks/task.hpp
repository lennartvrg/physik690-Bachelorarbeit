#ifndef TASK_HPP
#define TASK_HPP

#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <queue>
#include <optional>

#include "storage/storage.hpp"
#include "utils/utils.hpp"

namespace tasks {
	template<typename TStorage, typename TTask, typename TResult>
	requires std::is_base_of_v<Storage, TStorage> class Task {
	public:
		template<typename ... Args>
		explicit Task(const Config & config, Args && ... args) : config(config), counter(0), storage(std::make_shared<TStorage>(std::forward<Args>(args)...)) {
			std::cout << "[Task] Preparing execution by running migrations" << std::endl;
			storage->prepare_simulation(config);
		}

		virtual ~Task() {
			std::cout << "[Task] Saved a total of " << counter << " results" << std::endl;
		}

		void execute() {
			// The queue holds the task results
			std::mutex queue_mutex;
			std::queue<std::tuple<TTask, int64_t, int64_t, TResult>> queue {};

			// The maximum number of workers is equal to the hardware concurrency
			std::atomic_size_t workers { 0 };
			const auto max_workers = std::thread::hardware_concurrency();

			// As long as there are new tasks, we want to delegate the task to a new worker
			while (const std::optional<TTask> in = next_task(storage)) {
				workers.fetch_add(1, std::memory_order_seq_cst);

				// Starting to process the task on a new thread
				std::thread([&] (const TTask & value) {
					const auto start_time_ms = utils::timestamp_ms();
					const auto result = execute_task(value);
					const auto end_time_ms = utils::timestamp_ms();

					// Add the task result to the queue
					const std::unique_lock lock { queue_mutex };
					queue.push({ value, start_time_ms, end_time_ms, result });

					// Indicate that the thread is finished
					workers.fetch_sub(1, std::memory_order_seq_cst);
					workers.notify_one();
				}, *in).detach();

				// Wait until one worker has finished and save the queue
				while (workers == max_workers) {
					workers.wait(max_workers);
				}
				save_queue(queue, queue_mutex);
				storage->worker_keep_alive();
			}

			// Wait until all workers are finished and drain the remaining queue
			for (std::size_t value; (value = workers.load(std::memory_order_seq_cst)) != 0;) {
				workers.wait(value);
			}
			save_queue(queue, queue_mutex);
		}

	protected:
		const Config config;

		virtual std::optional<TTask> next_task(std::shared_ptr<TStorage> storage) = 0;

		virtual TResult execute_task(const TTask & task) = 0;

		virtual void save_task(std::shared_ptr<TStorage> storage, const TTask & task, int64_t start_time, int64_t end_time, const TResult & result) = 0;

	private:
		/// A counter to keep track of how many results have been saved to storage.
		std::size_t counter;

		/// The underlying storage engine for this task.
		std::shared_ptr<TStorage> storage;

		void save_queue(std::queue<std::tuple<TTask, int64_t, int64_t, TResult>> & queue, std::mutex & queue_mutex) {
			const std::unique_lock lock { queue_mutex };
			while (!queue.empty()) {
				const auto [task, start_time, end_time, result] = queue.front();
				queue.pop();

				save_task(storage, task, start_time, end_time, result);
				counter++;
			}
		}
	};
}

#endif //TASK_HPP

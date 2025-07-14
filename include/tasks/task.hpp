#ifndef TASK_HPP
#define TASK_HPP

#include <memory>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <queue>
#include <optional>
#include <utility>

#include "storage/storage.hpp"
#include "utils/utils.hpp"

namespace tasks {
	template<typename TStorage, typename TTask, typename TResult>
	requires std::is_base_of_v<Storage, TStorage> class Task {
	public:
		Task(Config config, const std::shared_ptr<TStorage> storage) : config(std::move(config)), storage(storage) {

		}

		virtual ~Task() {
			std::cout << "[Task] Saved a total of " << counter << " results" << std::endl;
		}

		void execute() {
			// Keeping track of worker threads
			std::vector<std::thread> workers;
			std::unique_lock lock_tasks { available_tasks_mutex };

			// Create worker threads and fetch initial tasks
			for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
				if (const auto task = next_task(this->storage)) {
					available_tasks.push(*task);
				}

				if (i == 0 && available_tasks.empty()) {
					return;
				}
				workers.emplace_back(&Task::execute_worker, this);
			}

			// Start workers
			lock_tasks.unlock();
			available_tasks_signal.notify_all();

			// Wait for results
			auto last_keep_alive = std::chrono::system_clock::now();
			while (!exit_flag) {
				std::unique_lock lock { available_results_signal_mutex };
				available_results_signal.wait_for(lock, std::chrono::seconds(1), [&] {
					const std::unique_lock result_lock { available_results_mutex };
					return !available_results.empty();
				});
				lock.unlock();

				// Save the complete queue
				save_queue(available_results, available_results_mutex);

				// Send a keep alive message every 10 seconds
				if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_keep_alive).count() >= 10) {
					last_keep_alive = std::chrono::system_clock::now();
					this->storage->worker_keep_alive();
				}

				// Fill missing tasks
				lock_tasks.lock();
				for (std::size_t i = 0; i < idle_workers; ++i) {
					if (const auto task = next_task(this->storage)) {
						available_tasks.push(*task);
					}
				}

				const auto empty = available_tasks.empty();
				lock_tasks.unlock();

				available_tasks_signal.notify_all();
				if (idle_workers == std::thread::hardware_concurrency() && empty) {
					exit_flag = true;
				}
			}

			// Wait for all workers to finish
			for (auto & worker : workers) {
				worker.join();
			}

			// Save the complete queue
			save_queue(available_results, available_results_mutex);
		}

	protected:
		const Config config;

		virtual std::optional<TTask> next_task(std::shared_ptr<TStorage> storage) = 0;

		virtual TResult execute_task(const TTask & task) = 0;

		virtual void save_task(std::shared_ptr<TStorage> storage, const TTask & task, int64_t start_time, int64_t end_time, const TResult & result) = 0;

	private:
		/// A counter to keep track of how many results have been saved to storage.
		std::size_t counter {0};

		/// The underlying storage engine for this task.
		std::shared_ptr<TStorage> storage;

		std::atomic_bool exit_flag;

		std::atomic_uint idle_workers;

		std::mutex available_tasks_mutex;

		std::queue<TTask> available_tasks;

		std::mutex available_tasks_signal_mutex;

		std::condition_variable available_tasks_signal;

		std::mutex available_results_mutex;

		std::queue<std::tuple<TTask, int64_t, int64_t, TResult>> available_results;

		std::mutex available_results_signal_mutex;

		std::condition_variable available_results_signal;

		void execute_worker() {
			while (!exit_flag) {
				// Signal that the worker is looking for a task
				++idle_workers;

				// Wait for an incoming task
				std::unique_lock lock { available_tasks_signal_mutex };
				available_tasks_signal.wait_for(lock, std::chrono::seconds(1), [&] {
					const std::unique_lock tasks_lock { available_tasks_mutex };
					return !available_tasks.empty() || exit_flag;
				});
				lock.unlock();

				// Worker busy and exit flag handling
				if (exit_flag) {
					break;
				}
				--idle_workers;

				// Get the task from the queue
				std::unique_lock lock_tasks { available_tasks_mutex };
				if (available_tasks.empty()) {
					continue;
				}
				const auto task = available_tasks.front();

				// Remove task from queue
				available_tasks.pop();
				lock_tasks.unlock();

				// Execute the task
				const auto start_time_ms = utils::timestamp_ms();
				const auto result = execute_task(task);
				const auto end_time_ms = utils::timestamp_ms();

				// Push to the result queue
				std::unique_lock lock_results { available_results_mutex };
				available_results.push({ task, start_time_ms, end_time_ms, result });

				// Signal the result is ready
				available_results_signal.notify_one();
			}
		}

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

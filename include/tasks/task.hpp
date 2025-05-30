#ifndef TASK_HPP
#define TASK_HPP

#include <memory>

#include "storage/storage.hpp"

namespace tasks {

	template<typename T> requires (std::is_base_of_v<Storage, T>) class Task {
	public:
		explicit Task(const Config & config) : config(config), storage(std::make_shared<T>()) {
			storage->prepare_simulation(config);
		}



	private:
		const Config config;
		std::shared_ptr<T> storage;
	};
}

#endif //TASK_HPP

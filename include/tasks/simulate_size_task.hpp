#ifndef SIMULATE_SIZE_TASK_HPP
#define SIMULATE_SIZE_TASK_HPP

#include "tasks/task.hpp"

namespace tasks {
	template<typename T> requires (std::is_base_of_v<Storage, T>)
	class SimulateSizeTask final : public Task<T> {
	public:
		explicit SimulateSizeTask(const Config & config) : Task<T>(config) {

		}
	};
}

#endif //SIMULATE_SIZE_TASK_HPP

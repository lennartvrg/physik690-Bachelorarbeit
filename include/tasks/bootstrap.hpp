#ifndef BOOTSTRAP_HPP
#define BOOTSTRAP_HPP

#include "task.hpp"
#include "storage/storage.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Bootstrap final : public Task<TStorage, std::tuple<Estimate, std::vector<double>>, std::tuple<double, double>> {
	public:
		template<typename ... Args>
		explicit Bootstrap(const Config & config, Args && ... args) : Task<TStorage, std::tuple<Estimate, std::vector<double>>, std::tuple<double, double>>(config, std::forward<Args>(args)...) {

		}
	protected:
		std::optional<std::tuple<Estimate, std::vector<double>>> next_task() override {
			return this->storage->next_estimate(this->config.simulation_id);
		}

		std::tuple<double, double> execute_task(const std::tuple<Estimate, std::vector<double>> & task) override {
			const auto [estimate, values] = task;
			std::mt19937 rng {std::random_device{}()};

			return analysis::bootstrap_blocked(rng, values, estimate.bootstrap_resamples);
		}

		void save_task(const std::tuple<Estimate, std::vector<double>> & task, const std::tuple<double, double> & result) override {
			std::cout << "ConfigurationId: " << get<0>(task).configuration_id << " | Type: " << get<0>(task).type << std::endl;
			this->storage->save_estimate(get<0>(task), get<0>(result), get<1>(result));
		}
	};
}

#endif //BOOTSTRAP_HPP

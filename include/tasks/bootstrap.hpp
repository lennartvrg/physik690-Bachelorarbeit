#ifndef BOOTSTRAP_HPP
#define BOOTSTRAP_HPP

#include "task.hpp"
#include "storage/storage.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Bootstrap final : public Task<TStorage, std::tuple<Estimate, std::vector<double_t>>, std::tuple<double_t, double_t>> {
	public:
		template<typename ... Args>
		explicit Bootstrap(const Config & config, Args && ... args) : Task<TStorage, std::tuple<Estimate, std::vector<double_t>>, std::tuple<double_t, double_t>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<std::tuple<Estimate, std::vector<double_t>>> next_task(std::shared_ptr<TStorage> storage) override {
			return storage->next_estimate(this->config.simulation_id);
		}

		std::tuple<double_t, double_t> execute_task(const std::tuple<Estimate, std::vector<double_t>> & task) override {
			XoshiroCpp::Xoshiro256Plus rng { std::random_device {}() };

			const auto [estimate, values] = task;
			return analysis::bootstrap_blocked(rng, values, estimate.bootstrap_resamples);
		}

		void save_task(std::shared_ptr<TStorage> storage, const std::tuple<Estimate, std::vector<double_t>> & task, int64_t start_time, int64_t end_time, const std::tuple<double_t, double_t> & result) override {
			const auto [estimate, _1] = task;
			const auto [mean, std_dev] = result;

			std::cout << "[Bootstrap] ConfigurationId: " << estimate.configuration_id << " | Type: " << estimate.type << std::endl;
			storage->save_estimate(estimate.configuration_id, start_time, end_time, estimate.type, mean, std_dev);
		}
	};
}

#endif //BOOTSTRAP_HPP

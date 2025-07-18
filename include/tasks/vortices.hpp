#ifndef VORTICES_HPP
#define VORTICES_HPP

#include <cstddef>

#include "tasks/task.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Vortices final : public Task<TStorage, std::tuple<std::size_t, std::size_t>, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>>> {
	public:
		template<typename ... Args>
		explicit Vortices(const Config & config, Args && ... args) : Task<TStorage, std::tuple<std::size_t, std::size_t>, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<std::tuple<std::size_t, std::size_t>> next_task(std::shared_ptr<TStorage> storage) override {
			return storage->next_vortex(this->config.simulation_id);
		}

		std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>> execute_task(const std::tuple<std::size_t, std::size_t> & pair) override {
			XoshiroCpp::Xoshiro256Plus rng { std::random_device {}() };
			const auto [vortex_id, size] = pair;

			std::size_t sweeps = 100000;
			Lattice lattice { size, 1.0, std::nullopt };

			// Thermalize
			std::cout << "[Vortices] Thermalizing for " << sweeps << " sweeps" << std::endl;
			algorithms::simulate(lattice, rng, sweeps, algorithms::METROPOLIS);

			// Transition from hot to cold state
			std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>> results;
			for (const auto temperature : utils::sweep_temperature_rev(0.0, 1.0, 120)) {
				std::cout << "[Vortices] Simulating at t " << std::fixed << std::setprecision(3) << temperature << std::endl;
				lattice.set_beta(1.0 / temperature);

				// Stay at temperature
				for (const std::size_t _ : std::views::iota(0, 20)) {
					algorithms::simulate(lattice, rng, 1, algorithms::METROPOLIS);
					results.emplace_back(temperature, ++sweeps, lattice.get_spins());
				}
			}

			// Wait for vortices to dissolve
			for (const auto _ : std::views::iota(0, 2400)) {
				algorithms::simulate(lattice, rng, 200, algorithms::METROPOLIS);
				results.emplace_back(get<0>(results.at(results.size() - 1)), sweeps += 200, lattice.get_spins());
			}

			return results;
		}

		void save_task(std::shared_ptr<TStorage> storage, const std::tuple<std::size_t, std::size_t> & task, [[maybe_unused]] int64_t start_time, [[maybe_unused]] int64_t end_time, const std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>> & results) override {
			storage->save_vortices(get<0>(task), results);
		}
	};
}

#endif //VORTICES_HPP

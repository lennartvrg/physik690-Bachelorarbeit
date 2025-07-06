#ifndef SIMULATION_HPP
#define SIMULATION_HPP

#include "observables/type.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"
#include "tasks/task.hpp"
#include "schemas/serialize.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Simulation final : public Task<TStorage, Chunk, std::tuple<std::vector<double_t>, observables::Map>> {
	public:
		template<typename ... Args>
		explicit Simulation(const Config & config, Args && ... args) : Task<TStorage, Chunk, std::tuple<std::vector<double_t>, observables::Map>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<Chunk> next_task(std::shared_ptr<TStorage> storage) override {
			return storage->next_chunk(this->config.simulation_id);
		}

		std::tuple<std::vector<double_t>, observables::Map> execute_task(const Chunk & chunk) override {
			XoshiroCpp::Xoshiro256Plus rng { std::random_device {}() };
			Lattice lattice {chunk.lattice_size, chunk.temperature.inverse(), chunk.spins};

			observables::Map results;
			for (auto [type, values] : algorithms::simulate(lattice, rng, chunk.sweeps, chunk.algorithm)) {
				const auto[tau, autocorrelation] = analysis::integrated_autocorrelation_time(values);
				results[type] = { tau, analysis::thermalize_and_block(values, tau, !chunk.first()), chunk.first() ? std::make_optional(autocorrelation) : std::nullopt };
			}
			return { lattice.get_spins(), results };
		}

		void save_task(std::shared_ptr<TStorage> storage, const Chunk & chunk, int64_t start_time, int64_t end_time, const std::tuple<std::vector<double_t>, observables::Map> & result) override {
			const auto [ spin_data, measurements ] = result;
			const auto spins = schemas::serialize(spin_data);

			std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> results;
			for (const auto & [ type, value ] : measurements) {
				const auto [tau, values, autocorrelation] = value;
				results.insert({ type, { tau, schemas::serialize(values), autocorrelation.transform(schemas::serialize) } });
			}

			std::cout << "[Simulation] " << chunk.algorithm << " | Size: " << chunk.lattice_size << " | ConfigurationId: " << chunk.configuration_id << " | Index: " << chunk.index << std::endl;
			storage->save_chunk(chunk, start_time, end_time, spins, results);
		}
	};
}

#endif //SIMULATION_HPP

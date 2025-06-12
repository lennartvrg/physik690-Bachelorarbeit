#ifndef SIMULATE_SIZE_TASK_HPP
#define SIMULATE_SIZE_TASK_HPP

#include "observables/type.hpp"
#include "algorithms/wolff.hpp"
#include "algorithms/metropolis.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"
#include "tasks/task.hpp"
#include "schemas/serialize.hpp"
#include "utils/utils.hpp"

namespace tasks {
	static algorithms::function sweeps[2] = { algorithms::metropolis, algorithms::wolff };

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
			Lattice lattice {chunk.lattice_size, chunk.temperature.inverse(), chunk.spins};
			openrand::Tyche rng { std::random_device{}(), 0 };

			auto [energy, magnets] = algorithms::simulate(lattice, chunk.sweeps, rng, sweeps[chunk.algorithm]);
			auto [energy_sqr, magnets_sqr] = std::make_tuple(utils::square_elements(energy), utils::square_elements(magnets));

			const auto [e_tau, _1] = analysis::integrated_autocorrelation_time(energy);
			const auto [m_tau, _2] = analysis::integrated_autocorrelation_time(magnets);

			const auto [e_sqr_tau, _3] = analysis::integrated_autocorrelation_time(energy_sqr);
			const auto [m_sqr_tau, _4] = analysis::integrated_autocorrelation_time(magnets_sqr);

			return { lattice.get_spins(), {
				{ observables::Type::Energy, { e_tau, analysis::thermalize_and_block(energy, e_tau, chunk.skip_thermalization()) } },
				{ observables::Type::EnergySquared, { e_tau, analysis::thermalize_and_block(energy_sqr, e_sqr_tau, chunk.skip_thermalization()) } },
				{ observables::Type::Magnetization, { m_tau, analysis::thermalize_and_block(magnets, m_tau, chunk.skip_thermalization()) } },
				{ observables::Type::MagnetizationSquared, { m_tau, analysis::thermalize_and_block(magnets_sqr, m_sqr_tau, chunk.skip_thermalization()) } }
			}};
		}

		void save_task(std::shared_ptr<TStorage> storage, const Chunk & chunk, int64_t start_time, int64_t end_time, const std::tuple<std::vector<double_t>, observables::Map> & result) override {
			const auto [ spin_data, measurements ] = result;
			const auto spins = schemas::serialize(spin_data);

			std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>>> results;
			for (const auto & [ type, value ] : measurements) {
				const auto [tau, values] = value;
				results.insert({ type, { tau, schemas::serialize(values) } });
			}

			std::cout << "[Simulation] " << chunk.algorithm << " | Size: " << chunk.lattice_size << " | ConfigurationId: " << chunk.configuration_id << " | Index: " << chunk.index << std::endl;
			storage->save_chunk(chunk, start_time, end_time, spins, results);
		}
	};
}

#endif //SIMULATE_SIZE_TASK_HPP

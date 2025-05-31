#ifndef SIMULATE_SIZE_TASK_HPP
#define SIMULATE_SIZE_TASK_HPP

#include "observables/type.hpp"
#include "algorithms/wolff.hpp"
#include "algorithms/metropolis.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"
#include "tasks/task.hpp"

namespace tasks {
	static algorithms::function sweeps[2] = { algorithms::metropolis, algorithms::wolff };

	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Simulation final : public Task<TStorage, Chunk, std::tuple<Lattice, observables::Map>> {

	public:
		template<typename ... Args>
		explicit Simulation(const Config & config, Args && ... args) : Task<TStorage, Chunk, std::tuple<Lattice, observables::Map>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<Chunk> next_task() override {
			return this->storage->next_chunk(this->config.simulation_id);
		}

		std::tuple<Lattice, observables::Map> execute_task(const Chunk & chunk) override {
			Lattice lattice {chunk.lattice_size, 1.0 / chunk.temperature, chunk.spins};
			std::mt19937 rng {std::random_device{}()};

			auto [energy, magnets] = algorithms::simulate(lattice, chunk.sweeps, rng, sweeps[chunk.algorithm]);

			const auto [e_tau, _1] = analysis::integrated_autocorrelation_time(energy);
			const auto [m_tau, _2] = analysis::integrated_autocorrelation_time(magnets);

			return { lattice, {
				{ observables::Type::Energy, analysis::thermalize_and_block(energy, e_tau) },
				{ observables::Type::Magnetization, analysis::thermalize_and_block(magnets, m_tau) }
			}};
		}

		void save_task(const Chunk & chunk, const std::tuple<Lattice, observables::Map> & result) override {
			std::cout << "ConfigurationId: " << chunk.configuration_id << " | ChunkId: " << chunk.chunk_id << " | Size: " << chunk.lattice_size << std::endl;
			this->storage->save_chunk(chunk, get<0>(result).get_spins(), get<1>(result));
		}
	};
}

#endif //SIMULATE_SIZE_TASK_HPP

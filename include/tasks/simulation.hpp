#ifndef SIMULATE_SIZE_TASK_HPP
#define SIMULATE_SIZE_TASK_HPP

#include <flatbuffers/flatbuffers.h>

#include "observables/type.hpp"
#include "algorithms/wolff.hpp"
#include "algorithms/metropolis.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"
#include "tasks/task.hpp"

#include "schemas/spins_generated.h"
#include "schemas/measurements_generated.h"

namespace tasks {
	static algorithms::function sweeps[2] = { algorithms::metropolis, algorithms::wolff };

	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Simulation final : public Task<TStorage, Chunk, std::tuple<std::vector<double>, observables::Map>> {

	public:
		template<typename ... Args>
		explicit Simulation(const Config & config, Args && ... args) : Task<TStorage, Chunk, std::tuple<std::vector<double>, observables::Map>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<Chunk> next_task() override {
			return this->storage->next_chunk(this->config.simulation_id);
		}

		std::tuple<std::vector<double>, observables::Map> execute_task(const Chunk & chunk) override {
			Lattice lattice {chunk.lattice_size, 1.0 / chunk.temperature, chunk.spins};
			std::mt19937 rng {std::random_device{}()};

			auto [energy, magnets] = algorithms::simulate(lattice, chunk.sweeps, rng, sweeps[chunk.algorithm]);

			const auto [e_tau, _1] = analysis::integrated_autocorrelation_time(energy);
			const auto [m_tau, _2] = analysis::integrated_autocorrelation_time(magnets);

			return { lattice.get_spins(), {
				{ observables::Type::Energy, analysis::thermalize_and_block(energy, e_tau) },
				{ observables::Type::Magnetization, analysis::thermalize_and_block(magnets, m_tau) }
			}};
		}

		void save_task(const Chunk & chunk, const std::tuple<std::vector<double>, observables::Map> & result) override {
			const auto [ spins, measurements ] = result;
			std::cout << "Size: " << chunk.lattice_size << " | ConfigurationId: " << chunk.configuration_id << " | Index: " << chunk.index << std::endl;

			flatbuffers::FlatBufferBuilder spin_builder { sizeof(std::vector<double>) + sizeof(double) * spins.size() };
			const auto offset = spin_builder.CreateVector(spins.data(), spins.size());

			const auto spins_offset = schemas::CreateSpins(spin_builder, offset);
			spin_builder.Finish(spins_offset);

			std::vector<flatbuffers::FlatBufferBuilder> builders;
			std::map<observables::Type, std::span<uint8_t>> results;

			for (const auto & [ type, values ] : measurements) {
				flatbuffers::FlatBufferBuilder result_builder { sizeof(std::vector<double>) + sizeof(double) * values.size() };
				const auto result_offset = result_builder.CreateVector(values);

				const auto measurements_offset = schemas::CreateMeasurements(result_builder, result_offset);
				result_builder.Finish(measurements_offset);

				results.insert({ type, result_builder.GetBufferSpan() });
				builders.push_back(std::move(result_builder));
			}

			this->storage->save_chunk(chunk, spin_builder.GetBufferSpan(), results);
		}
	};
}

#endif //SIMULATE_SIZE_TASK_HPP

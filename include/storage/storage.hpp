#ifndef STORAGE_HPP
#define STORAGE_HPP

#include "config.hpp"
#include "next_derivative.hpp"
#include "observables/type.hpp"
#include "storage/estimate.hpp"
#include "storage/chunk.hpp"

class Storage {
public:
	virtual ~Storage() = default;

	virtual bool prepare_simulation(Config config) = 0;

	virtual std::optional<std::tuple<std::size_t, algorithms::Algorithm, std::size_t>> next_vortex(int simulation_id) = 0;

	virtual void save_vortices(std::size_t vortex_id, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>>) = 0;

	virtual std::optional<Chunk> next_chunk(int simulation_id) = 0;

	virtual void save_chunk(const Chunk & chunk, int32_t thread_num, int64_t start_time, int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> & results) = 0;

	virtual std::optional<std::tuple<Estimate, std::vector<double_t>>> next_estimate(int simulation_id) = 0;

	virtual void save_estimate(int configuration_id, int32_t thread_num, int64_t start_time, int64_t end_time, observables::Type type, double_t mean, double_t std_dev) = 0;

	virtual std::optional<NextDerivative> next_derivative(int simulation_id) = 0;

	virtual void worker_keep_alive() = 0;

	virtual void synchronize_workers() = 0;
};

#endif //STORAGE_HPP

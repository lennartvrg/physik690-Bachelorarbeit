#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <cstddef>
#include <map>
#include <unordered_set>

#include "algorithms/algorithms.hpp"

struct AlgorithmConfig {
	const std::size_t num_chunks;
	const std::size_t sweeps_per_chunk;
	const std::unordered_set<std::size_t> sizes;
};

struct Config {
	static Config from_file(std::string_view path);

	const int32_t simulation_id;
	const std::size_t bootstrap_resamples;

	const int32_t max_temperature;
	const int32_t temperature_steps;
	const double_t max_depth;

	const std::unordered_set<std::size_t> vortex_sizes;

	const std::map<algorithms::Algorithm, AlgorithmConfig> algorithms;
};

#endif //CONFIG_HPP

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

	const std::size_t simulation_id;
	const std::size_t bootstrap_resamples;

	const double max_temperature;
	const std::size_t temperature_steps;

	const std::map<algorithms::Algorithm, AlgorithmConfig> algorithms;
};

#endif //CONFIG_HPP

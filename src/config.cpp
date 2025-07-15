#include "config.hpp"

#include <iostream>
#include <toml++/toml.hpp>

AlgorithmConfig parse_algorithm_config(const toml::node_view<const toml::node> node) noexcept {
	std::unordered_set<std::size_t> sizes {};
	node["sizes"].as_array()->for_each([&] <typename T>(T && el) {
		if constexpr (toml::is_integer<T>) {
			sizes.insert(el.template value_or<std::size_t>(10));
		}
	});

	const auto num_chunks = node["num_chunks"].value_or<std::size_t>(1);
	const auto sweeps_per_chunk = node["sweeps_per_chunk"].value_or<std::size_t>(100000);

	return AlgorithmConfig { num_chunks, sweeps_per_chunk, sizes };
}

Config Config::from_file(const std::string_view path) {
	const auto config = toml::parse_file(path);

	const auto simulation_id = config["simulation"]["simulation_id"].value_or<int32_t>(0);
	const auto bootstrap_resamples = config["simulation"]["bootstrap_resamples"].value_or<std::size_t>(100000);

	const auto max_temperature = config["temperature"]["max"].value_or<int32_t>(3);
	const auto temperature_steps = config["temperature"]["steps"].value_or<int32_t>(64);
	const auto max_depth = config["temperature"]["max_depth"].value_or<double_t>(1);

	const auto engine = static_cast<StorageEngine>(config["storage"]["engine"].value_or<int32_t>(1));
	const auto connection_string = config["storage"]["connection_string"].value_or<std::string>("output/data.db");

	std::unordered_set<std::size_t> vortex_sizes {};
	config["vortices"]["sizes"].as_array()->for_each([&] <typename T>(T && el) {
		if constexpr (toml::is_integer<T>) {
			vortex_sizes.insert(el.template value_or<std::size_t>(10));
		}
	});

	std::map<algorithms::Algorithm, AlgorithmConfig> algorithms;
	if (const auto node = config["metropolis"]) algorithms.emplace(algorithms::METROPOLIS, parse_algorithm_config(node));
	if (const auto node = config["wolff"]) algorithms.emplace(algorithms::WOLFF, parse_algorithm_config(node));

	return Config { engine, connection_string, simulation_id, bootstrap_resamples, max_temperature, temperature_steps, max_depth, vortex_sizes, algorithms };
}

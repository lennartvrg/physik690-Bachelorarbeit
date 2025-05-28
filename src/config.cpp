#include "config.hpp"

#include <iostream>
#include <toml++/toml.hpp>

AlgorithmConfig parse_algorithm_config(const toml::node_view<const toml::node> node) {
	std::set<std::size_t> sizes {};
	node["sizes"].as_array()->for_each([&] <typename T>(T && el) {
		if constexpr (toml::is_integer<T>) {
			sizes.insert(el.template value_or<std::size_t>(10));
		}
	});

	return AlgorithmConfig {
		node["num_chunks"].value_or<std::size_t>(1),
		node["sweeps_per_chunk"].value_or<std::size_t>(100000),
		sizes,
	};
}

Config Config::from_file(const std::string_view path) {
	const auto config = toml::parse_file(path);

	const auto simulation_id = config["simulation"]["simulation_id"].value_or<std::size_t>(0);
	const auto bootstrap_resamples = config["simulation"]["bootstrap_resamples"].value_or<std::size_t>(100000);

	std::map<algorithms::Algorithm, AlgorithmConfig> algorithms;
	if (const auto node = config["metropolis"]) algorithms.emplace(algorithms::METROPOLIS, parse_algorithm_config(node));
	if (const auto node = config["wolff"]) algorithms.emplace(algorithms::WOLFF, parse_algorithm_config(node));

	return Config { simulation_id, bootstrap_resamples, 3.0, 64, algorithms };
}



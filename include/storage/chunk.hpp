#ifndef CHUNK_HPP
#define CHUNK_HPP

#include <optional>
#include <vector>

#include "../algorithms/algorithms.hpp"

struct Chunk final {
	const int configuration_id;

	const int index;

	const algorithms::Algorithm algorithm;

	const std::size_t lattice_size;

	const double temperature;

	const std::size_t sweeps;

	std::optional<std::vector<double>> spins;
};

#endif //CHUNK_HPP

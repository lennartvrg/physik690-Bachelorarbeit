#ifndef CHUNK_HPP
#define CHUNK_HPP

#include <optional>
#include <vector>

#include "algorithms/algorithms.hpp"

struct Chunk final {
	const int configuration_id;

	const int index;

	const algorithms::Algorithm algorithm;

	const std::size_t lattice_size;

	const double_t temperature;

	const std::size_t sweeps;

	std::optional<std::vector<double_t>> spins;

	[[nodiscard]] bool first() const {
		return index == 1;
	}
};

#endif //CHUNK_HPP

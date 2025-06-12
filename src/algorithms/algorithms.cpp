#include <cassert>
#include <cmath>

#include "algorithms/algorithms.hpp"

constexpr std::string_view AlgorithmStrings[] =
{
	"Metropolis",
	"Wolff"
};

std::ostream& algorithms::operator<<(std::ostream& out, const Algorithm value) {
	return out << AlgorithmStrings[static_cast<std::size_t>(value)];
}

std::tuple<std::vector<double_t>, std::vector<double_t>> algorithms::simulate(Lattice & lattice, const std::size_t sweeps, openrand::Tyche & rng, const function & sweep) noexcept {
	auto current_energy = lattice.energy();
	auto [current_magnet_cos, current_magnet_sin] = lattice.magnetization();

	const auto norm = 1.0 / static_cast<double_t>(lattice.num_sites());
	std::vector<double_t> energies (sweeps), magnets (sweeps);

	for (std::size_t i = 0; i < sweeps; ++i) {
		const auto [chg_energy, chg_magnet] = sweep(lattice, rng);
		current_magnet_cos += get<0>(chg_magnet);
		current_magnet_sin += get<1>(chg_magnet);
		current_energy += chg_energy;

		energies[i] = current_energy * norm;
		magnets[i] = std::sqrt(std::pow(current_magnet_cos, 2.0) + std::pow(current_magnet_sin, 2.0)) * norm;
	}

	return {energies, magnets};
}
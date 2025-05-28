#include "algorithms/algorithms.hpp"

std::tuple<std::vector<double>, std::vector<double>> algorithms::simulate(Lattice & lattice, const std::size_t sweeps, std::mt19937 &rng, const function & sweep) noexcept {
	auto current_energy = lattice.energy();
	auto [current_magnet_cos, current_magnet_sin] = lattice.magnetization();

	std::vector<double> energies{}, magnets{};
	for (std::size_t i = 0; i < sweeps; i++) {
		const auto [chg_energy, chg_magnet] = sweep(lattice, rng);
		current_magnet_cos += get<0>(chg_magnet);
		current_magnet_sin += get<1>(chg_magnet);
		current_energy += chg_energy;

		energies.push_back(current_energy / static_cast<double>(lattice.num_sites()));
		magnets.push_back(std::sqrt(current_magnet_cos * current_magnet_cos + current_magnet_sin * current_magnet_sin) /
						  static_cast<double>(lattice.num_sites()));
	}

	return {energies, magnets};
}
#include <cassert>
#include <cmath>

#include "algorithms/algorithms.hpp"
#include "algorithms/metropolis.hpp"
#include "algorithms/wolff.hpp"

constexpr std::string_view AlgorithmStrings[] =
{
	"Metropolis",
	"Wolff"
};

std::ostream& algorithms::operator<<(std::ostream& out, const Algorithm value) {
	return out << AlgorithmStrings[static_cast<std::size_t>(value)];
}

static std::unordered_map<observables::Type, std::vector<double_t>> simulate_metropolis(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng, const std::size_t sweeps) {
	auto current_energy = lattice.energy();
	auto current_helicity_modulus = lattice.helicity_modulus();
	auto [current_magnet_cos, current_magnet_sin] = lattice.magnetization();

	const auto norm = 1.0 / static_cast<double_t>(lattice.num_sites());
	std::vector<double_t> energies (sweeps), helicity_modulus (sweeps), magnets (sweeps);

	for (std::size_t i = 0; i < sweeps; ++i) {
		const auto [chg_energy, chg_helicity_modulus, chg_magnet] = algorithms::metropolis(lattice, rng);
		current_magnet_cos += get<0>(chg_magnet);
		current_magnet_sin += get<1>(chg_magnet);
		current_energy += chg_energy;
		current_helicity_modulus = lattice.helicity_modulus();

		energies[i] = current_energy * norm;
		helicity_modulus[i] = std::pow(current_helicity_modulus, 2.0) * norm;
		magnets[i] = std::sqrt(std::pow(current_magnet_cos, 2.0) + std::pow(current_magnet_sin, 2.0)) * norm;
	}

	return {{ observables::Type::Energy, energies }, { observables::Type::Magnetization, magnets }, { observables::Type::HelicityModulusFraction, helicity_modulus }};
}

static std::unordered_map<observables::Type, std::vector<double_t>> simulate_wolff(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng, const std::size_t sweeps) {
	auto current_energy = lattice.energy();
	auto current_helicity_modulus = lattice.helicity_modulus();
	auto [current_magnet_cos, current_magnet_sin] = lattice.magnetization();

	const auto norm = 1.0 / static_cast<double_t>(lattice.num_sites());
	std::vector<double_t> energies (sweeps), helicity_modulus (sweeps), magnets (sweeps), clusters (sweeps);

	for (std::size_t i = 0; i < sweeps; ++i) {
		const auto [chg_energy, chg_helicity_modulus, chg_magnet, cluster_size] = algorithms::wolff(lattice, rng);
		current_magnet_cos += get<0>(chg_magnet);
		current_magnet_sin += get<1>(chg_magnet);
		current_energy += chg_energy;
		current_helicity_modulus = lattice.helicity_modulus();

		clusters[i] = cluster_size * norm;
		energies[i] = current_energy * norm;
		helicity_modulus[i] = std::pow(current_helicity_modulus, 2.0) * norm;
		magnets[i] = std::sqrt(std::pow(current_magnet_cos, 2.0) + std::pow(current_magnet_sin, 2.0)) * norm;
	}

	return {
		{observables::Type::Energy, energies}, {observables::Type::Magnetization, magnets},
		{observables::Type::HelicityModulusFraction, helicity_modulus}, {observables::Type::ClusterSize, clusters}
	};
}

std::unordered_map<observables::Type, std::vector<double_t>> algorithms::simulate(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng, const std::size_t sweeps, const Algorithm algorithm) noexcept {
	std::unordered_map<observables::Type, std::vector<double_t>> result;
	for (auto & [type, values] : algorithm == WOLFF ? simulate_wolff(lattice, rng, sweeps) : simulate_metropolis(lattice, rng, sweeps)) {
		if (type == observables::Energy) {
			result[observables::EnergySquared] = utils::square_elements(values);
		} else if (type == observables::Magnetization) {
			result[observables::MagnetizationSquared] = utils::square_elements(values);
		}
		result[type] = values;
	}
	return result;
}
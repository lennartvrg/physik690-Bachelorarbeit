#include "metropolis.hpp"

static std::tuple<double, std::tuple<double, double>> sweep(Lattice &lattice, std::mt19937 &rng) noexcept {
    auto [chg_energy, chg_magnet_cos, chg_magnet_sin] = std::tuple<double, double, double>{0.0, 0.0, 0.0};

    for (std::size_t i = 0; i < lattice.num_sites(); i++) {
        auto angle = algorithms::ANGLE(rng);
        auto energy_diff = lattice.energy_diff(i, angle);
        auto [magnet_cos_diff, magnet_sin_diff] = lattice.magnetization_diff(i, angle);

        if (lattice.acceptance(energy_diff) > algorithms::ACCEPTANCE(rng)) {
            chg_energy += energy_diff;
            chg_magnet_cos += magnet_cos_diff;
            chg_magnet_sin += magnet_sin_diff;
            lattice.set(i, angle);
        }
    }

    return {chg_energy, {chg_magnet_cos, chg_magnet_sin}};
}

std::tuple<std::vector<double>, std::vector<double>> algorithms::metropolis(Lattice &lattice, std::size_t sweeps, std::mt19937 &rng) noexcept {
    double current_energy = lattice.energy();
    auto [current_magnet_cos, current_magnet_sin] = lattice.magnetization();

    std::vector<double> energies{}, magnets{};
    for (std::size_t i = 0; i < sweeps; i++) {
        auto [chg_energy, chg_magnet] = sweep(lattice, rng);
        current_magnet_cos += get<0>(chg_magnet);
        current_magnet_sin += get<1>(chg_magnet);
        current_energy += chg_energy;

        energies.push_back(current_energy / static_cast<double>(lattice.num_sites()));
        magnets.push_back(std::sqrt(current_magnet_cos * current_magnet_cos + current_magnet_sin * current_magnet_sin) /
                          static_cast<double>(lattice.num_sites()));
    }

    return {energies, magnets};
}

#include "metropolis.hpp"

/**
 * Performs a single Metropolis-Hastings sweep over the given lattice. A single sweep visits every lattice site in order
 * of the underlying vector and proposes a new angle for the spin at the site. The new state is accepted based on the
 * energy difference of the proposed new angle. If the state is accepted the site spin is updated and the total change
 * of energy and magnetization is updated.
 *
 * @brief Performs a single Metropolis-Hastings sweep over the given lattice.
 *
 * @param lattice The lattice over which the sweep should be made.
 * @param rng The random number generator to use for the acceptance probability.
 * @return The total change of energy and magnetization once every lattice site is visited.
 */
static std::tuple<double, std::tuple<double, double>> sweep(Lattice & lattice, std::mt19937 & rng) noexcept {
    // Prepares the result objects containing the total change of energy and magnetization
    double chg_energy = 0.0, chg_magnet_cos = 0.0, chg_magnet_sin = 0.0;

    // Go over all lattice sites and propose a new angle for the spin at the site
    for (std::size_t i = 0; i < lattice.num_sites(); i++) {
        const auto angle = algorithms::ANGLE(rng);

        // Calculate the difference the proposed angle would make
        const auto energy_diff = lattice.energy_diff(i, angle);
        const auto [magnet_cos_diff, magnet_sin_diff] = lattice.magnetization_diff(i, angle);

        // Check acceptance probability min(1.0, -exp{-BETA * H}) and update lattice site / results
        if (lattice.acceptance(energy_diff) > algorithms::ACCEPTANCE(rng)) {
            chg_energy += energy_diff;
            chg_magnet_cos += magnet_cos_diff;
            chg_magnet_sin += magnet_sin_diff;
            lattice.set(i, angle);
        }
    }

    return {chg_energy, {chg_magnet_cos, chg_magnet_sin}};
}

std::tuple<std::vector<double>, std::vector<double>> algorithms::metropolis(Lattice & lattice, const std::size_t sweeps, std::mt19937 &rng) noexcept {
    double current_energy = lattice.energy();
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

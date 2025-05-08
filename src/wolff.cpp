#include <stack>
#include <unordered_map>
#include <cmath>
#include <unordered_set>
#include <iostream>

#include "wolff.hpp"

static std::tuple<double, std::tuple<double, double>> sweep(Lattice &lattice, std::mt19937 &rng) noexcept {
    // Prepares the result objects containing the total change of energy and magnetization
    auto chg_energy = 0.0, chg_magnet_cos = 0.0, chg_magnet_sin = 0.0;
    std::uniform_int_distribution<std::size_t> sites{0, lattice.num_sites()};

    const std::size_t random_site = sites(rng);
    const auto reference_angle = algorithms::ANGLE(rng);
    const auto reference_x = std::cos(reference_angle);
    const auto reference_y = std::sin(reference_angle);

    std::unordered_set visited { random_site };
    std::stack<std::size_t> stack {{ random_site }};

    while (!stack.empty()) {
        // Get the next cluster site
        const auto i = stack.top();
        stack.pop();

        // Get the old spin
        const auto old_angle = lattice.get(i);
        const auto spin_x = std::cos(old_angle);
        const auto spin_y = std::sin(old_angle);

        // Reflect the angle along hyperplane perpendicular to the reference angle r
        const auto scaler = 2.0 * std::cos(reference_angle - old_angle);
        const auto flipped_x = spin_x - scaler * reference_x;
        const auto flipped_y = spin_y - scaler * reference_y;

        // Negating the parameters, adding PI and performing a mod 2PI maps the atan2 output domain [-PI, PI] to [0, 2PI)
        const auto flipped_angle = std::fmod(std::atan2(-flipped_y, -flipped_x) + std::numbers::pi, algorithms::TWO_PI);

        // Calculate observable difference of proposed spin
        const auto energy_diff = lattice.energy_diff(i, flipped_angle);
        const auto [magnet_cos_diff, magnet_sin_diff] = lattice.magnetization_diff(i, flipped_angle);
        lattice.set(i, flipped_angle);

        // Update observables
        chg_energy += energy_diff;
        chg_magnet_cos += magnet_cos_diff;
        chg_magnet_sin += magnet_sin_diff;

        // Find neighboring spins
        const std::size_t neighbours[4] = {(i + 1) % lattice.num_sites(),
                                           (i + lattice.num_sites() - 1) % lattice.num_sites(),
                                           (i + lattice.side_length()) % lattice.num_sites(),
                                           (i + lattice.num_sites() - lattice.side_length()) % lattice.num_sites()};

        // Go through neighboring spins which have not yet been visited
        for (const std::size_t j : neighbours) {
            if (!visited.contains(j)) {
                // Calculate dot product of neighbors
                const auto prop_i= std::cos(old_angle - reference_angle);
                const auto prop_j= std::cos(lattice.get(j) - reference_angle);

                // Add spin to the cluster with P = 1 - exp{min{0.0,-2*BETA*(ox*r)*(oy*r)}} and mark as visited
                if (const auto accept = 1.0 - std::exp(std::min(0.0, -2.0 * lattice.get_beta() * prop_i * prop_j)); accept > algorithms::ACCEPTANCE(rng)) {
                    visited.insert(j);
                    stack.push(j);
                }
            }
        }
    }

    return {chg_energy, {chg_magnet_cos, chg_magnet_sin}};
}

std::tuple<std::vector<double>, std::vector<double>>
algorithms::wolff(Lattice &lattice, const std::size_t sweeps, std::mt19937 &rng) noexcept {
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

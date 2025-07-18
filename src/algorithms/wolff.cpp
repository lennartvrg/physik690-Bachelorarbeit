#include <queue>
#include <cmath>
#include <unordered_set>

#include "algorithms/wolff.hpp"

std::tuple<double_t, double_t, std::tuple<double_t, double_t>, std::int32_t, std::size_t> algorithms::wolff(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng) noexcept {
    // Prepares the result objects containing the total change of energy and magnetization
    auto chg_energy = 0.0, chg_helicity_modulus = 0.0, chg_magnet_cos = 0.0, chg_magnet_sin = 0.0;
    std::uniform_int_distribution<std::size_t> sites {0, lattice.num_sites() - 1 };

    // Pick a random starting site and random reference angle
    const auto random_site = sites(rng);
    const auto reference_angle = XoshiroCpp::DoubleFromBits(rng()) * N_PI<2>;

    // Keep track of visited sites and site yet to flip. Start with the random site from above.
    std::unordered_set visited { random_site };
    std::queue<std::size_t> queue {{ random_site }};

    // While there are still new sites to visit -> Pop from stack
    std::int32_t cluster_size = 0;
    while (!queue.empty()) {
        // Get the next cluster site
        const auto i = queue.front();
        queue.pop();

        // Increment cluster size
        cluster_size++;

        // Negating the parameters, adding PI and performing a mod 2PI maps the atan2 output domain [-PI, PI] to [0, 2PI)
        const auto old_angle = lattice[i];
        const auto flipped_angle = std::fmod(algorithms::N_PI<3> + 2.0 * reference_angle - old_angle, algorithms::N_PI<2>);

        // Calculate observable difference of proposed spin
        const auto energy_diff = lattice.energy_diff(i, flipped_angle);
        const auto helicity_modulus = lattice.helicity_modulus_diff(i, flipped_angle);
        const auto [magnet_cos_diff, magnet_sin_diff] = lattice.magnetization_diff(i, flipped_angle);
        lattice.set(i, flipped_angle);

        // Update observables
        chg_energy += energy_diff;
        chg_helicity_modulus += helicity_modulus;
        chg_magnet_cos += magnet_cos_diff;
        chg_magnet_sin += magnet_sin_diff;

        // Find neighboring spins
        const std::size_t neighbors[4] = {lattice.shift_col(i, 1), lattice.shift_col(i, -1), lattice.shift_row(i, 1), lattice.shift_row(i, -1)};

        // Calculate dot product of neighbors for the old angle
        const auto prop_i= std::cos(old_angle - reference_angle);

        // Go through neighboring spins which have not yet been visited
        for (const std::size_t j : neighbors) {
            if (!visited.contains(j)) {
                // Calculate dot product of neighbors for the new angle
                const auto prop_j= std::cos(lattice[j] - reference_angle);

                // Add spin to the cluster with P = 1 - exp{min{0.0,-2*BETA*(ox*r)*(oy*r)}} and mark as visited
                if (const auto accept = 1.0 - std::exp(std::min(0.0, -2.0 * lattice.get_beta() * prop_i * prop_j)); accept > XoshiroCpp::DoubleFromBits(rng())) {
                    visited.emplace(j);
                    queue.emplace(j);
                }
            }
        }
    }

    return {chg_energy, chg_helicity_modulus, {chg_magnet_cos, chg_magnet_sin}, cluster_size, visited.size()};
}

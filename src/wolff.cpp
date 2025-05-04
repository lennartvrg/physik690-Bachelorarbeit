#include <deque>

#include "wolff.hpp"

static std::tuple<double, std::tuple<double, double>> sweep(Lattice &lattice, std::mt19937 &rng) noexcept {
    std::uniform_int_distribution<std::size_t> sites {0, lattice.num_sites()};

    const std::size_t random_site = sites(rng);
    const double reference_angle = algorithms::ANGLE(rng);

    std::deque<std::size_t> cluster { random_site };
    for (std::size_t i : cluster) {

    }
}

std::tuple<std::vector<double>, std::vector<double>> algorithms::wolff(Lattice & lattice, std::size_t sweeps, std::mt19937 & rng) noexcept {

}

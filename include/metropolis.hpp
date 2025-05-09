#ifndef METROPOLIS_HPP
#define METROPOLIS_HPP

#include "algorithms.hpp"

namespace algorithms {
    std::tuple<double, std::tuple<double, double>> metropolis(Lattice & lattice, std::mt19937 & rng) noexcept;
}

#endif //METROPOLIS_HPP

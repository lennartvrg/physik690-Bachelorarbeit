#ifndef METROPOLIS_HPP
#define METROPOLIS_HPP

#include "algorithm.hpp"

namespace algorithms {
    std::tuple<std::vector<double>, std::vector<double>> metropolis(Lattice & lattice, std::size_t sweeps, std::mt19937 & rng) noexcept;
}

#endif //METROPOLIS_HPP

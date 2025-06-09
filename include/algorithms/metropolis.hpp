#ifndef METROPOLIS_HPP
#define METROPOLIS_HPP

#include "algorithms/algorithms.hpp"

namespace algorithms {
    std::tuple<double_t, std::tuple<double_t, double_t>> metropolis(Lattice & lattice, openrand::Tyche & rng) noexcept;
}

#endif //METROPOLIS_HPP

#ifndef WOLFF_HPP
#define WOLFF_HPP

#include "algorithms/algorithms.hpp"

namespace algorithms {
    std::tuple<double_t, std::tuple<double_t, double_t>> wolff(Lattice & lattice, openrand::Tyche & rng) noexcept;
}

#endif //WOLFF_HPP

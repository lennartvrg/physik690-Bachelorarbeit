#ifndef WOLFF_HPP
#define WOLFF_HPP

#include "algorithms.hpp"

namespace algorithms {
    std::tuple<double, std::tuple<double, double>> wolff(Lattice & lattice, std::mt19937 & rng) noexcept;
}

#endif //WOLFF_HPP

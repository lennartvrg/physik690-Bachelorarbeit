#ifndef WOLFF_HPP
#define WOLFF_HPP

#include "algorithm.hpp"

namespace algorithms {
    std::tuple<std::vector<double>, std::vector<double>> wolff(Lattice & lattice, std::size_t sweeps, std::mt19937 & rng) noexcept;
}

#endif //WOLFF_HPP

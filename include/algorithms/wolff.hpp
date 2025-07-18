#ifndef WOLFF_HPP
#define WOLFF_HPP

#include "algorithms/algorithms.hpp"

namespace algorithms {
    std::tuple<double_t, double_t, std::tuple<double_t, double_t>, std::int32_t, std::size_t> wolff(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng) noexcept;
}

#endif //WOLFF_HPP

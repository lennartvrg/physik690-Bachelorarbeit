#ifndef ALGORITHM_HPP
#define ALGORITHM_HPP

#include <tuple>
#include <functional>
#include <random>

#include "lattice.hpp"

namespace algorithms {
    static thread_local std::uniform_real_distribution<double> ACCEPTANCE {0.0, 1.0};

    static thread_local std::uniform_real_distribution<double> ANGLE {0.0, 2.0 * std::numbers::pi};

    using function = std::function<std::tuple<std::vector<double>, std::vector<double>>(Lattice&, std::size_t, std::mt19937&)>;
}

#endif //ALGORITHM_HPP

#ifndef ALGORITHM_HPP
#define ALGORITHM_HPP

#include <tuple>
#include <functional>
#include <random>

#include "lattice.hpp"

namespace algorithms {
    enum Algorithm { METROPOLIS, WOLFF };

    template<const std::size_t N> constexpr double N_PI = N * std::numbers::pi;

    /**
     * The acceptance probability on the uniform distribution [0.0, 1.0).
     */
    static thread_local std::uniform_real_distribution<double> ACCEPTANCE {0.0, 1.0};

    static thread_local std::uniform_real_distribution<double> ANGLE {0.0, N_PI<2>};

    using function = std::function<std::tuple<double, std::tuple<double, double>>(Lattice&, std::mt19937&)>;

    std::tuple<std::vector<double>, std::vector<double>> simulate(Lattice & lattice, std::size_t sweeps, std::mt19937 &rng, const function &sweep) noexcept;
}

#endif //ALGORITHM_HPP

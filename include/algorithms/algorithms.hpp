#ifndef ALGORITHM_HPP
#define ALGORITHM_HPP

#include <tuple>
#include <functional>
#include <random>
#include <openrand/tyche.h>

#include "lattice.hpp"

namespace algorithms {
    enum Algorithm { METROPOLIS = 0, WOLFF = 1 };

    std::ostream& operator<<(std::ostream& out, Algorithm value);

    /**
     * Helper constant for calculating N * PI.
     *
     * @tparam N The factor of PI
     */
    template<const std::size_t N> constexpr double_t N_PI = N * std::numbers::pi;

    /**
     * The acceptance probability on the uniform distribution [0.0, 1.0).
     */
    static thread_local std::uniform_real_distribution ACCEPTANCE {0.0, 1.0};

    /**
     * The random angle on the uniform distribution [0, 2PI)
     */
    static thread_local std::uniform_real_distribution ANGLE {0.0, N_PI<2>};

    using function = std::function<std::tuple<double_t, std::tuple<double_t, double_t>>(Lattice&, openrand::Tyche&)>;

    std::tuple<std::vector<double_t>, std::vector<double_t>> simulate(Lattice & lattice, std::size_t sweeps, openrand::Tyche & rng, const function & sweep) noexcept;
}

#endif //ALGORITHM_HPP

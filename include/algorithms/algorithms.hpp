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
    template<const std::size_t N> constexpr double_t N_PI = static_cast<double_t>(N) * std::numbers::pi;

    using function = std::function<std::tuple<double_t, std::tuple<double_t, double_t>>(Lattice&, openrand::Tyche&)>;

    std::tuple<std::vector<double_t>, std::vector<double_t>> simulate(Lattice & lattice, std::size_t sweeps, openrand::Tyche & rng, const function & sweep) noexcept;
}

#endif //ALGORITHM_HPP

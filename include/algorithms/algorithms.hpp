#ifndef ALGORITHM_HPP
#define ALGORITHM_HPP

#include <functional>
#include <random>
#include <XoshiroCpp.hpp>

#include "lattice.hpp"
#include "observables/type.hpp"

namespace algorithms {
    enum Algorithm { METROPOLIS = 0, WOLFF = 1 };

    std::ostream& operator<<(std::ostream& out, Algorithm value);

    /**
     * Helper constant for calculating N * PI.
     *
     * @tparam N The factor of PI
     */
    template<const std::size_t N> constexpr double_t N_PI = static_cast<double_t>(N) * std::numbers::pi;

    std::unordered_map<observables::Type, std::vector<double_t>> simulate(Lattice & lattice, XoshiroCpp::Xoshiro256Plus & rng, std::size_t sweeps, Algorithm algorithm) noexcept;
}

#endif //ALGORITHM_HPP

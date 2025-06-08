#ifndef BOOSTRAP_HPP
#define BOOSTRAP_HPP

#include <tuple>
#include <span>
#include <random>

namespace analysis {
    std::vector<double_t> thermalize_and_block(const std::span<double_t> & data, double_t tau, bool skip_thermalization = false);

    std::tuple<double_t, double_t> bootstrap_blocked(std::mt19937 &rng, const std::vector<double_t> & blocked, std::size_t n);
}

#endif //BOOSTRAP_HPP

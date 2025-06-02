#ifndef BOOSTRAP_HPP
#define BOOSTRAP_HPP

#include <tuple>
#include <span>
#include <random>

namespace analysis {
    std::vector<double> thermalize_and_block(const std::span<double> & data, double tau);

    std::tuple<double, double> bootstrap_blocked(std::mt19937 &rng, const std::vector<double> & blocked, std::size_t n);
}

#endif //BOOSTRAP_HPP

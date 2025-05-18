#ifndef BOOSTRAP_HPP
#define BOOSTRAP_HPP

#include <tuple>
#include <span>
#include <random>

namespace analysis {
    std::tuple<double, double> bootstrap(std::mt19937 &rng, const std::span<double> & data, const double tau, const std::size_t n);
}

#endif //BOOSTRAP_HPP

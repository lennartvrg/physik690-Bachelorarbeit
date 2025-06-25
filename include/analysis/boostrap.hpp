#ifndef BOOSTRAP_HPP
#define BOOSTRAP_HPP

#include <tuple>
#include <span>
#include <random>
#include <XoshiroCpp.hpp>

namespace analysis {
    std::vector<double_t> thermalize_and_block(const std::span<double_t> & data, double_t tau, bool skip_thermalization = false);

    std::tuple<double_t, double_t> bootstrap_blocked(XoshiroCpp::Xoshiro256Plus & rng, const std::vector<double_t> & blocked, std::size_t n);
}

#endif //BOOSTRAP_HPP

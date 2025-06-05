#include <algorithm>
#include <ranges>

#include "analysis/boostrap.hpp"

std::vector<double> blocking(const std::span<double> & data, const double tau) {
    const auto size = static_cast<std::size_t>(std::ceil(tau));

    std::vector<double> result {};
    std::ranges::transform(data | std::ranges::views::chunk(size), std::back_inserter(result), [&] (const auto & x) {
        return result.emplace_back(std::ranges::fold_left(x, 0.0, std::plus()) / static_cast<double>(size));
    });

    return result;
}

std::span<double> thermalize(const std::span<double> & data, const double tau) {
    const auto offset = static_cast<std::size_t>(std::ceil(3 * tau));
    return data.subspan(offset, std::ranges::size(data) - offset);
}

std::vector<double> analysis::thermalize_and_block(const std::span<double> & data, const double tau, const bool skip_thermalization) {
    return blocking(skip_thermalization ? data : thermalize(data, tau), tau);
}

double sample_with_replacement(std::mt19937 & rng, const std::vector<double> & data, const std::size_t n) {
    std::uniform_int_distribution<std::size_t> distrib {0, std::ranges::size(data) - 1};
    std::vector<double> draws (n);

    std::ranges::generate(draws, [&] { return data.at(distrib(rng)); });
    return std::ranges::fold_left(draws, 0.0, std::plus()) / static_cast<double>(draws.size());
}

std::tuple<double, double> analysis::bootstrap_blocked(std::mt19937 & rng, const std::vector<double> & blocked, const std::size_t n) {
    const std::size_t count = std::ranges::size(blocked);

    std::vector<double> resamples (n);
    std::ranges::generate(resamples, [&] {
        return sample_with_replacement(rng, blocked, n);
    });

    const auto mean = std::ranges::fold_left(blocked, 0.0, std::plus()) / static_cast<double>(count);
    const auto std_dev = std::ranges::fold_left(resamples, 0.0, [&] (const auto sum, const double x) {
        return sum + std::pow(x - mean, 2);
    } ) / static_cast<double>(count - 1);

    return {mean, std_dev};
}

#include <algorithm>
#include <ranges>

#include "analysis/boostrap.hpp"

std::vector<double_t> blocking(const std::span<double_t> & data, const double_t tau) {
    const auto stride = static_cast<std::size_t>(std::ceil(tau));
    const auto norm = 1.0 / static_cast<double_t>(stride);

    std::vector<double_t> result (std::ceil(static_cast<double_t>(data.size()) / static_cast<double_t>(stride)));
    std::ranges::transform(data | std::ranges::views::chunk(stride), result.begin(), [&] (const auto & x) {
        return std::ranges::fold_left(x, 0.0, std::plus()) * norm;
    });

    return result;
}

std::span<double_t> thermalize(const std::span<double_t> & data, const double_t tau) {
    const auto offset = static_cast<std::size_t>(std::ceil(3 * tau));
    return data.subspan(offset, data.size() - offset);
}

std::vector<double_t> analysis::thermalize_and_block(const std::span<double_t> & data, const double_t tau, const bool skip_thermalization) {
    return blocking(skip_thermalization ? data : thermalize(data, tau), tau);
}

double_t sample_with_replacement(std::mt19937 & rng, const std::vector<double_t> & data, const std::size_t n) {
    std::uniform_int_distribution<std::size_t> distrib {0, data.size() - 1};
    std::vector<double_t> draws (n);

    std::ranges::generate(draws, [&] { return data[distrib(rng)]; });
    return std::ranges::fold_left(draws, 0.0, std::plus()) / static_cast<double_t>(draws.size());
}

std::tuple<double_t, double_t> analysis::bootstrap_blocked(std::mt19937 & rng, const std::vector<double_t> & blocked, const std::size_t n) {
    const std::size_t count = std::ranges::size(blocked);

    std::vector<double_t> resamples (n);
    std::ranges::generate(resamples, [&] {
        return sample_with_replacement(rng, blocked, n);
    });

    const auto mean = std::ranges::fold_left(blocked, 0.0, std::plus()) / static_cast<double_t>(count);
    const auto std_dev = std::ranges::fold_left(resamples, 0.0, [&] (const auto sum, const double_t x) {
        return sum + std::pow(x - mean, 2);
    } ) / static_cast<double_t>(count - 1);

    return {mean, std_dev};
}

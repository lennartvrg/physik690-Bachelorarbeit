#include <algorithm>
#include <ranges>
#include <cmath>

#include "analysis/boostrap.hpp"

std::vector<double_t> blocking(const std::span<double_t> & data, const double_t tau) {
    const auto stride = static_cast<std::size_t>(std::ceil(tau));

    const auto num_chunks = static_cast<std::size_t>(std::ceil(static_cast<double_t>(data.size()) / static_cast<double_t>(stride)));
    const auto chunked = data | std::ranges::views::chunk(stride);

    std::vector<double_t> result (num_chunks);
    std::ranges::transform(chunked, result.begin(), [] (const auto & x) {
        return std::ranges::fold_left(x, 0.0, std::plus()) / static_cast<double_t>(x.size());
    });

    return result;
}

std::span<double_t> thermalize(const std::span<double_t> & data, const double_t tau) {
    const auto offset = 3 * static_cast<std::size_t>(std::ceil(tau));
    return data.subspan(offset, data.size() - offset);
}

std::vector<double_t> analysis::thermalize_and_block(const std::span<double_t> & data, const double_t tau, const bool skip_thermalization) {
    return blocking(skip_thermalization ? data : thermalize(data, tau), tau);
}

double_t sample_with_replacement(openrand::Philox & rng, const std::vector<double_t> & data) {
    return std::ranges::fold_left(data, 0.0, [&] (const auto sum, const auto) {
        return sum + data[rng.range(data.size())];
    }) / static_cast<double_t>(data.size());
}

std::tuple<double_t, double_t> analysis::bootstrap_blocked(openrand::Philox & rng, const std::vector<double_t> & blocked, const std::size_t n) {
    std::vector<double_t> resamples (n);
    std::ranges::generate(resamples, [&] {
        return sample_with_replacement(rng, blocked);
    });

    const auto mean = std::ranges::fold_left(blocked, 0.0, std::plus()) / static_cast<double_t>(blocked.size());
    const auto tmp = std::ranges::fold_left(resamples, 0.0, std::plus()) / static_cast<double_t>(resamples.size());

    const auto std_dev = std::ranges::fold_left(resamples, 0.0, [&] (const auto sum, const auto x) {
        return sum + std::pow(tmp - x, 2.0);
    } ) / static_cast<double_t>(resamples.size() - 1);

    return {mean, std_dev};
}

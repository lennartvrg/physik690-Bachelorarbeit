#include <algorithm>
#include <ranges>

#include "analysis/boostrap.hpp"

std::vector<double> blocking(const std::span<double> & data, const double tau) {
    std::vector<double> result {};
    std::ranges::transform(data | std::ranges::views::chunk(tau), std::back_inserter(result), [&](const auto & x) {
        return result.emplace_back(std::ranges::fold_left(x, 0.0, std::plus()));
    });
    return result;
}

std::span<double> thermalize(const std::span<double> & data, const double tau) {
    const auto offset = static_cast<std::size_t>(std::ceil(3 * tau));
    return data.subspan(offset, std::ranges::size(data) - offset);
}

std::tuple<double, double> analysis::bootstrap(std::mt19937 & rng, const std::span<double> & data, const double tau, const double n) {
    const auto blocked = blocking(thermalize(data, tau), tau);
    const int count = std::ranges::size(blocked);

    std::vector<double> resamples (n);
    std::uniform_int_distribution<> distrib {0, count - 1};

    std::ranges::generate(resamples, [&] {
        auto result = 0.0;
        for (int i = 0; i < count; ++i) {
            result += blocked.at(distrib(rng));
        }
        return result / count;
    });

    const auto mean = std::ranges::fold_left(blocked, 0.0, std::plus()) / std::ranges::size(blocked);
    const auto stddev_sqr = std::ranges::fold_left(resamples, 0.0, [&] (const auto sum, const double x) {
        return sum + std::pow(x - mean, 2);
    }) / (std::ranges::size(resamples) - 1);

    return {mean, std::sqrt(stddev_sqr)};
}

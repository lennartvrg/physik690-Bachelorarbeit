#include <algorithm>
#include <unistd.h>
#include <climits>
#include <simde/x86/sse2.h>
#include <filesystem>
#include <fstream>
#include <ranges>
#include <chrono>

#include "utils/utils.hpp"

std::string utils::hostname() {
    char buffer[HOST_NAME_MAX + 1];
    gethostname(buffer, HOST_NAME_MAX + 1);

    const auto end = std::find(buffer, buffer + sizeof(buffer), '\0');
    return { buffer, end };
}

int64_t utils::timestamp_ms() {
    const auto since_epoch = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(since_epoch).count();
}

double_t utils::mm256_reduce_add_pd(const simde__m256d v) {
    simde__m128d low = simde_mm256_castpd256_pd128(v);
    const simde__m128d high = simde_mm256_extractf128_pd(v, 1);
    low = simde_mm_add_pd(low, high);

    const simde__m128d high64 = simde_mm_unpackhi_pd(low, low);
    return simde_mm_cvtsd_f64(simde_mm_add_sd(low, high64));
}

std::vector<double_t> utils::sweep_through_temperature(const double_t max_temperature, const std::size_t steps) {
    std::vector<double_t> result (steps);
    const auto norm = 1.0 / static_cast<double_t>(steps);

    std::ranges::generate(result, [&, n = 0.0] mutable {
        return n += max_temperature *norm;
    });
    return result;
}

std::vector<double_t> utils::square_elements(const std::span<double> & span) {
    std::vector<double> result (span.size());
    std::ranges::transform(span, result.begin(), [] (const double_t v) { return std::pow(v, 2); });
    return result;
}
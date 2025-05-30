#include <algorithm>
#include <unistd.h>
#include <climits>
#include <simde/x86/sse2.h>
#include <filesystem>
#include <fstream>

#include "utils/utils.hpp"

std::string utils::hostname() {
    char buffer[HOST_NAME_MAX + 1];
    gethostname(buffer, HOST_NAME_MAX + 1);

    const auto end = std::find(buffer, buffer + sizeof(buffer), '\0');
    return { buffer, end };
}


double utils::mm256_reduce_add_pd(const simde__m256d v) {
    simde__m128d low = simde_mm256_castpd256_pd128(v);
    const simde__m128d high = simde_mm256_extractf128_pd(v, 1);
    low = simde_mm_add_pd(low, high);

    const simde__m128d high64 = simde_mm_unpackhi_pd(low, low);
    return simde_mm_cvtsd_f64(simde_mm_add_sd(low, high64));
}

std::vector<double> utils::sweep_through_temperature(const double max_temperature, const std::size_t steps) {
    std::vector<double> result (steps);
    std::ranges::generate(result, [&, n = 0.0] mutable {
        return n += max_temperature / static_cast<double>(steps);
    });
    return result;
}

void utils::write_output_csv(const std::ostringstream & measurements, const std::string & file_name, const std::string & headers) {
    std::filesystem::create_directories("output");

    std::ofstream output;
    output.open("output/" + file_name + ".csv");

    output << headers << std::endl << measurements.str();
    output.close();
}

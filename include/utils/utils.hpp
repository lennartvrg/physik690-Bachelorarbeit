#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <simde/x86/avx.h>

namespace utils {
	std::string hostname();

	int64_t timestamp_ms();

	double mm256_reduce_add_pd(simde__m256d v);

	std::vector<double_t> sweep_through_temperature(double_t max_temperature, std::size_t steps);

	std::vector<double_t> square_elements(const std::span<double_t> & span);
}

#endif //UTILS_HPP

#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <generator>
#include <simde/x86/avx.h>

#include "utils/ratio.hpp"

namespace utils {
	std::string hostname();

	int64_t timestamp_ms();

	double_t mm256_reduce_add_pd(simde__m256d v);

	std::generator<ratio> sweep_through_temperature(int32_t max_temperature, int32_t steps);

	std::vector<double_t> square_elements(const std::span<double_t> & span);
}

#endif //UTILS_HPP

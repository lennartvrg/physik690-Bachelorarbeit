#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <simde/x86/avx.h>

namespace utils {
	std::string hostname();

	double mm256_reduce_add_pd(simde__m256d v);

	std::vector<double> sweep_through_temperature(double max_temperature, std::size_t steps);

	std::vector<double> square_elements(const std::span<double> & span);
}

#endif //UTILS_HPP

#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <generator>
#include <boost/align/aligned_allocator.hpp>
#include <simde/x86/avx.h>

namespace utils {
	template<typename T>
	using aligned_vector = std::vector<T, boost::alignment::aligned_allocator<T, 32>>;

	std::string hostname();

	int64_t timestamp_ms();

	double_t mm256_reduce_add_pd(simde__m256d v);

	std::generator<double_t> sweep_temperature(double_t min_temperature, double_t max_temperature, int32_t steps, bool end_inclusive = true);

	std::generator<double_t> sweep_temperature_rev(double_t min_temperature, double_t max_temperature, int32_t steps, bool end_inclusive = true);

	std::vector<double_t> square_elements(const std::span<double_t> & span);

	void sleep_between(std::int32_t lower, std::int32_t upper);
}

#endif //UTILS_HPP

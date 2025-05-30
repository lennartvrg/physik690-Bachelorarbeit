#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <simde/x86/avx.h>

namespace utils {
	std::string hostname();

	double mm256_reduce_add_pd(simde__m256d v);

	std::vector<double> sweep_through_temperature(double max_temperature, std::size_t steps);

	void write_output_csv(const std::ostringstream & measurements, const std::string & file_name, const std::string & headers);
}

#endif //UTILS_HPP

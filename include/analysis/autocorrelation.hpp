#ifndef AUTOCORRELATION_HPP
#define AUTOCORRELATION_HPP

#include <cmath>
#include <tuple>
#include <vector>
#include <span>

namespace analysis {
	std::tuple<double_t, std::vector<double_t>> integrated_autocorrelation_time(const std::span<double_t> & data);
}

#endif //AUTOCORRELATION_HPP

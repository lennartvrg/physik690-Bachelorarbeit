#ifndef AUTOCORRELATION_HPP
#define AUTOCORRELATION_HPP

#include <tuple>
#include <vector>
#include <span>

namespace analysis {
	std::tuple<double, std::vector<double>> integrated_autocorrelation_time(const std::span<double> & data);
}

#endif //AUTOCORRELATION_HPP

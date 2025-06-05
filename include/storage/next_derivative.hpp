#ifndef DERIVATIVE_HPP
#define DERIVATIVE_HPP

#include "observables/type.hpp"

struct NextDerivative {
	const int configuration_id;

	const observables::Type type;

	const double temperature;

	const double mean;

	const double std_dev;

	const double square_mean;

	const double square_std_dev;
};

#endif //DERIVATIVE_HPP

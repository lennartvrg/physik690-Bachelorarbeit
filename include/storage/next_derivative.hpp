#ifndef DERIVATIVE_HPP
#define DERIVATIVE_HPP

#include "observables/type.hpp"

struct NextDerivative {
	const int configuration_id;

	const observables::Type type;

	const double temperature;

	const double mean;

	const double square_mean;
};

#endif //DERIVATIVE_HPP

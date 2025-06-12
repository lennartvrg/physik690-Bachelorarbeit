#ifndef DERIVATIVE_HPP
#define DERIVATIVE_HPP

#include "observables/type.hpp"

struct NextDerivative {
	const int configuration_id;

	const observables::Type type;

	const utils::ratio temperature;

	const double_t mean;

	const double_t std_dev;

	const double_t square_mean;

	const double_t square_std_dev;
};

#endif //DERIVATIVE_HPP

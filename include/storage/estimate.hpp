#ifndef ESTIMATE_HPP
#define ESTIMATE_HPP

#include "observables/type.hpp"

struct Estimate {
	const int configuration_id;

	const observables::Type type;

	const std::size_t bootstrap_resamples;
};

#endif //ESTIMATE_HPP

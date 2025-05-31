#ifndef TYPE_HPP
#define TYPE_HPP

#include <map>
#include <vector>

namespace observables {
	enum Type {
		Energy = 0,
		EnergySquared = 1,
		SpecificHeat = 2,
		Magnetization = 3,
		MagnetizationSquared = 4,
		MagneticSusceptibility = 5,
	};

	using Map = std::map<Type, std::vector<double>>;
}

#endif //TYPE_HPP

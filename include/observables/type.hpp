#ifndef TYPE_HPP
#define TYPE_HPP

#include <map>
#include <vector>

namespace observables {
	enum Type {
		Energy = 0,
		EnergySquared = 1,
		Magnetization = 2,
		MagnetizationSquared = 3,
		SpecificHeat = 4,
		MagneticSusceptibility = 5,
	};

	using Map = std::map<Type, std::tuple<double, std::vector<double>>>;
}

#endif //TYPE_HPP

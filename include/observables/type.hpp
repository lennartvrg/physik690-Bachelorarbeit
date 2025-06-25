#ifndef TYPE_HPP
#define TYPE_HPP

#include <cmath>
#include <ostream>
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
		HelicityModulus = 6,
		ClusterSize = 7,
	};

	std::ostream& operator<<(std::ostream& out, Type value);

	using Map = std::map<Type, std::tuple<double_t, std::vector<double_t>, std::optional<std::vector<double_t>>>>;
}

#endif //TYPE_HPP

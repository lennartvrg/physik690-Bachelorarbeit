#include "observables/type.hpp"

constexpr std::string_view TypeStrings[] =
{
	"Energy",
	"Energy squared",
	"Magnetization",
	"Magnetization squared",
	"Specific heat",
	"Magnetic susceptibility"
};

std::ostream& observables::operator<<(std::ostream& out, const Type value) {
	return out << TypeStrings[static_cast<std::size_t>(value)];
}

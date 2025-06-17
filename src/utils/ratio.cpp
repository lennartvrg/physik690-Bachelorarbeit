#include <cmath>
#include "utils/ratio.hpp"

utils::ratio::ratio(const int numerator, const int denominator) : numerator(numerator), denominator(denominator) {

}

double_t utils::ratio::approx() const {
	return static_cast<double_t>(numerator) / static_cast<double_t>(denominator);
}

utils::ratio utils::ratio::square() const {
	return { static_cast<int>(std::pow(numerator, 2)), static_cast<int>(std::powl(denominator, 2)) };
}

utils::ratio utils::ratio::inverse() const {
	return { denominator, numerator };
}

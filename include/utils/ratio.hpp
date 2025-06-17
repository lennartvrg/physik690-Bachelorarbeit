#ifndef RATIO_HPP
#define RATIO_HPP

#include <cmath>
#include <cstdint>

namespace utils {
	struct ratio {
		ratio(int numerator, int denominator);

		[[nodiscard]] double_t approx() const;
		[[nodiscard]] ratio inverse() const;
		[[nodiscard]] ratio square() const;

		const int32_t numerator;
		const int32_t denominator;
	};
}

#endif //RATIO_HPP

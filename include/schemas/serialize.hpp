#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <cstdint>
#include <cmath>
#include <vector>
#include <span>

namespace schemas {
	std::vector<uint8_t> serialize(const std::span<const double_t> & data);

	std::vector<double_t> deserialize(const void * data);
}

#endif //SERIALIZE_HPP

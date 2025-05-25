#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <cstddef>
#include <tuple>

#include "algorithms.hpp"

class Storage {
public:
	virtual ~Storage() = default;

	virtual std::optional<std::tuple<algorithms::Algorithm, std::size_t>> next_configuration() = 0;
};

#endif //STORAGE_HPP

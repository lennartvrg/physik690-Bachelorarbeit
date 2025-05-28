#ifndef STORAGE_HPP
#define STORAGE_HPP

#include "config.hpp"
#include "chunk.hpp"

class Storage {
public:
	virtual ~Storage() = default;

	virtual void prepare_simulation(Config config) = 0;

	virtual std::optional<Chunk> next_chunk(int simulation_id) = 0;
};

#endif //STORAGE_HPP

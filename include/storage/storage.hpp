#ifndef STORAGE_HPP
#define STORAGE_HPP

#include "config.hpp"
#include "chunk.hpp"
#include "observables/type.hpp"

class Storage {
public:
	virtual ~Storage() = default;

	virtual void prepare_simulation(Config config) = 0;

	virtual std::optional<Chunk> next_chunk(int simulation_id) = 0;

	virtual void save_chunk(const Chunk & chunk, const std::vector<double> & spins, const observables::Map & results) = 0;

	virtual void worker_keep_alive() = 0;
};

#endif //STORAGE_HPP

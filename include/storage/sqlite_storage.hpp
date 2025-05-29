#ifndef SQLITE_STORAGE_HPP
#define SQLITE_STORAGE_HPP

#include <SQLiteCpp/Database.h>

#include "storage.hpp"

class SQLiteStorage final : public Storage {
public:
	SQLiteStorage();

	void prepare_simulation(Config config) override;

	std::optional<Chunk> next_chunk(int simulation_id) override;

private:
	int worker_id;
	SQLite::Database db;
};

#endif //SQLITE_STORAGE_HPP

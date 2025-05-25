#ifndef SQLITE_STORAGE_HPP
#define SQLITE_STORAGE_HPP

#include <tuple>
#include <SQLiteCpp/Database.h>

#include "storage.hpp"

class SQLiteStorage final : public Storage {
public:
	SQLiteStorage();

	std::optional<std::tuple<algorithms::Algorithm, std::size_t>> next_configuration() override;

private:
	SQLite::Database db;
};

#endif //SQLITE_STORAGE_HPP

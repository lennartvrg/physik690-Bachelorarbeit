#ifndef SQLITE_HPP
#define SQLITE_HPP

#include <tuple>
#include <SQLiteCpp/Database.h>

#include "storage.hpp"

class SQLite2: public Storage {
public:
	SQLite2() : db("output/data.db3", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE) { }

	std::tuple<algorithms::Algorithm, std::size_t> next_configuration() override;

private:
	SQLite::Database db;
};

#endif //SQLITE_HPP

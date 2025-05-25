#include "storage/sqlite_storage.hpp"

#include <iostream>

#include "SQLiteCpp/Transaction.h"

static std::string Migrations = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	sweeps_per_chunk		INTEGER				NOT NULL DEFAULT (100000) CHECK (sweeps_per_chunk > 0),
	max_sweeps				INTEGER				NOT NULL DEFAULT (1000000) CHECK (max_sweeps > 0),
	created_at				DATETIME			NOT NULL DEFAULT (datetime()),

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
);

CREATE TABLE IF NOT EXISTS "configurations" (
	configuration_ID		INTEGER				NOT NULL,

	simulation_id			INTEGER				NOT NULL,
	algorithm				INTEGER				NOT NULL CHECK (algorithm = 0 OR algorithm = 1),
	lattice_size			INTEGER				NOT NULL CHECK (lattice_size > 0),
	temperature				REAL				NOT NULL CHECK (temperature > 0.0),

	CONSTRAINT "PK.Configurations_ConfigurationId" PRIMARY KEY (configuration_ID),
	CONSTRAINT "FK.Configurations_SimulationId"	FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id)
);

CREATE UNIQUE INDEX "IX.Configurations_SimulationId_Algorithm_LatticeSize_Temperature" ON "configurations" (simulation_id, algorithm, lattice_size, temperature);
)~~~~~~";

SQLiteStorage::SQLiteStorage() : db("../output/data.db", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE) {
	try {
		SQLite::Transaction transaction(db);
		db.exec(Migrations);
		transaction.commit();
	} catch (std::exception &e) {
		std::cout << "Failed to migrate database. SQLite exception: " << e.what() << std::endl;
		std::current_exception();
	}
}

std::optional<std::tuple<algorithms::Algorithm, std::size_t>> SQLiteStorage::next_configuration() {
	try {
		return { {algorithms::Algorithm::METROPOLIS, 20} };
	} catch (std::exception & e) {
		std::cout << "SQLite exception: " << e.what() << std::endl;
		return {};
	}
}

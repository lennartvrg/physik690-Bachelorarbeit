#include "storage/sqlite_storage.hpp"

#include <iostream>

#include "utils.hpp"
#include "SQLiteCpp/Transaction.h"

static std::string Migrations = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	bootstrap_resamples		INTEGER				NOT NULL DEFAULT (100000) CHECK (bootstrap_resamples > 0),
	created_at				DATETIME			NOT NULL DEFAULT (datetime()),

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
);

CREATE TRIGGER IF NOT EXISTS "TRG.ClearEstimates"
AFTER UPDATE OF bootstrap_resamples ON "simulations"
FOR EACH ROW WHEN OLD.bootstrap_resamples != NEW.bootstrap_resamples
BEGIN
	DELETE FROM "estimates" WHERE configuration_id IN (
		SELECT configuration_id FROM "configurations"
		WHERE simulation_id = NEW.simulation_id
	);
END;

CREATE TABLE IF NOT EXISTS "metadata" (
	metadata_id				INTEGER				NOT NULL,

	simulation_id			INTEGER				NOT NULL,
	algorithm				INTEGER				NOT NULL CHECK (algorithm = 0 OR algorithm = 1),
	num_chunks				INTEGER				NOT NULL DEFAULT (10) CHECK (num_chunks > 0),
	sweeps_per_chunk		INTEGER				NOT NULL DEFAULT (100000) CHECK (sweeps_per_chunk > 0),

	CONSTRAINT "PK.Metadata_SimulationId" PRIMARY KEY (metadata_id AUTOINCREMENT),
	CONSTRAINT "FK.Metadata_SimulationId" FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Metadata_SimulationId_Algorithm" ON "metadata" (simulation_id, algorithm);


CREATE TABLE IF NOT EXISTS "configurations" (
	configuration_ID		INTEGER				NOT NULL,

	simulation_id			INTEGER				NOT NULL,
	metadata_id				INTEGER				NOT NULL,
	lattice_size			INTEGER				NOT NULL CHECK (lattice_size > 0),
	temperature				REAL				NOT NULL CHECK (temperature > 0.0),

	CONSTRAINT "PK.Configurations_ConfigurationId" PRIMARY KEY (configuration_ID AUTOINCREMENT),
	CONSTRAINT "FK.Configurations_SimulationId"	FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id),
	CONSTRAINT "FK.Configurations_MetadataId" FOREIGN KEY (metadata_id) REFERENCES "metadata" (metadata_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Configurations_MetadataId_Algorithm_LatticeSize_Temperature" ON "configurations" (simulation_id, metadata_id, lattice_size, temperature);


CREATE TABLE IF NOT EXISTS "types" (
	type_id					INTEGER				NOT NULL,

	name					TEXT				NOT NULL,

	CONSTRAINT "PK.Types_TypeId" PRIMARY KEY (type_id)
);

INSERT OR IGNORE INTO "types" (type_id, name) VALUES (0, 'Energy'), (1, 'Energy Squared'), (2, 'Specific Heat'), (3, 'Magnetization'), (4, 'Magnetization Squared'), (5, 'Magnetic Susceptibility');


CREATE TABLE IF NOT EXISTS "chunks" (
	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	spins					TEXT				NOT NULL CHECK (json_valid(spins, 6) = 1 AND json_type(spins) = 'array'),

	CONSTRAINT "PK.Chunks_ConfigurationId_Index" PRIMARY KEY ("configuration_id", "index"),
	CONSTRAINT "FK.Chunks_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id)
);


CREATE TABLE IF NOT EXISTS "results" (
	chunk_id				INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	value					REAL				NOT NULL,

	CONSTRAINT "PK.Results_ChunkId_TypeId_Index" PRIMARY KEY (chunk_id, type_id, "index"),
	CONSTRAINT "FK.Results_ChunkId" FOREIGN KEY (chunk_id) REFERENCES "chunks" (chunk_id),
	CONSTRAINT "FK.Results_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);


CREATE TABLE IF NOT EXISTS "autocorrelation" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	tau						REAL				NOT NULL,

	CONSTRAINT "PK.Autocorrelation_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "FK.Autocorrelation_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Autocorrelation_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);


CREATE TABLE IF NOT EXISTS "estimates" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	mean					REAL				NOT NULL,
	std_dev					REAL				NOT NULL,

	CONSTRAINT "PK.Estimates_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "FK.Estimates_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Estimates_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);
)~~~~~~";

SQLiteStorage::SQLiteStorage() : db("output/data.db", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE) {
	try {
		SQLite::Transaction transaction(db);
		db.exec(Migrations);
		transaction.commit();
	} catch (std::exception &e) {
		std::cout << "Failed to migrate database. SQLite exception: " << e.what() << std::endl;
		std::terminate();
	}
}

static std::string InsertSimulationQuery = R"~~~~~~(
INSERT INTO "simulations" (simulation_id, bootstrap_resamples) VALUES (@simulation_id, @bootstrap_resamples)
ON CONFLICT DO UPDATE SET bootstrap_resamples = @bootstrap_resamples
)~~~~~~";

static std::string InsertMetadataQuery = R"~~~~~~(
INSERT INTO "metadata" (simulation_id, algorithm, num_chunks, sweeps_per_chunk) VALUES (@simulation_id, @algorithm, @num_chunks, @sweeps_per_chunk)
ON CONFLICT DO UPDATE SET num_chunks = @num_chunks WHERE num_chunks < @num_chunks RETURNING metadata_id
)~~~~~~";

static std::string InsertConfigurationsQuery = R"~~~~~~(
INSERT INTO "configurations" (simulation_id, metadata_id, lattice_size, temperature) VALUES (@simulation_id, @metadata_id, @lattice_size, @temperature)
ON CONFLICT DO NOTHING
)~~~~~~";

void SQLiteStorage::prepare_simulation(const Config config) {
	try {
		SQLite::Transaction transaction(db);

		SQLite::Statement simulation { db, InsertSimulationQuery };
		simulation.bind("@simulation_id", static_cast<int>(config.simulation_id));
		simulation.bind("@bootstrap_resamples", static_cast<int>(config.bootstrap_resamples));
		simulation.exec();

		SQLite::Statement metadata { db, InsertMetadataQuery };
		SQLite::Statement configurations { db, InsertConfigurationsQuery };

		for (const auto & [key, value] : config.algorithms) {
			metadata.bind("@simulation_id", static_cast<int>(config.simulation_id));
			metadata.bind("@algorithm", key);
			metadata.bind("@num_chunks", static_cast<int>(value.num_chunks));
			metadata.bind("@sweeps_per_chunk", static_cast<int>(value.sweeps_per_chunk));

			const auto metadata_id = metadata.executeStep() ? metadata.getColumn(0).getInt() : -1;
			metadata.reset();

			for (const auto size : value.sizes) {
				for (const auto temperature : sweep_through_temperature(config.max_temperature, config.temperature_steps)) {
					configurations.bind("@simulation_id", static_cast<int>(config.simulation_id));
					configurations.bind("@metadata_id", metadata_id);
					configurations.bind("@lattice_size", static_cast<int>(size));
					configurations.bind("@temperature", temperature);

					configurations.exec();
					configurations.reset();
				}
			}
		}

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "Failed to prepare simulation. SQLite exception: " << e.what() << std::endl;
		std::terminate();
	}
}


static std::string NextChunkQuery = R"~~~~~~(
SELECT c.configuration_ID, m.algorithm, c.lattice_size, c.temperature, m.sweeps_per_chunk, k.spins
FROM simulations s
INNER JOIN configurations c on s.simulation_id = c.simulation_id
INNER JOIN metadata m ON c.metadata_id = m.metadata_id
LEFT JOIN (
    SELECT k.configuration_id, k.spins, MAX(k."index") AS num_chunks
    FROM chunks k
) k ON c.configuration_ID = k.configuration_id
WHERE s.simulation_id = ? AND IfNull(k.num_chunks, 0) < m.num_chunks
ORDER BY c.lattice_size DESC LIMIT 1;
)~~~~~~";

std::optional<Chunk> SQLiteStorage::next_chunk(const int simulation_id) {
	try {
		SQLite::Statement query(db, NextChunkQuery);
		query.bind(1, simulation_id);

		if (query.executeStep()) {
			return std::optional<Chunk>({
				query.getColumn(0).getInt(),
				static_cast<algorithms::Algorithm>(query.getColumn(1).getInt()),
				static_cast<std::size_t>(query.getColumn(2).getInt()),
				query.getColumn(3).getDouble(),
				static_cast<std::size_t>(query.getColumn(4).getInt()),
				std::nullopt
			});
		}

	} catch (std::exception & e) {
		std::cout << "SQLite exception: " << e.what() << std::endl;
	}
	return std::nullopt;
}

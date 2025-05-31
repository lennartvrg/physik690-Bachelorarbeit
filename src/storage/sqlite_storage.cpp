#include <nlohmann/json.hpp>
#include <iostream>
#include <SQLiteCpp/Transaction.h>

#include "utils/utils.hpp"
#include "storage/sqlite_storage.hpp"

constexpr std::string_view Migrations = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	bootstrap_resamples		INTEGER				NOT NULL DEFAULT (100000) CHECK (bootstrap_resamples > 0),
	created_at				DATETIME			NOT NULL DEFAULT (datetime()),

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
);

CREATE TRIGGER IF NOT EXISTS "TRG.ClearEstimates"
AFTER UPDATE OF "bootstrap_resamples" ON "simulations"
FOR EACH ROW WHEN OLD."bootstrap_resamples" != NEW."bootstrap_resamples"
BEGIN
	DELETE FROM "estimates" WHERE "configuration_id" IN (
		SELECT "configuration_id" FROM "configurations"
		WHERE "simulation_id" = NEW."simulation_id"
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

CREATE TABLE IF NOT EXISTS "workers" (
	worker_id				INTEGER				NOT NULL,

	name					TEXT				NOT NULL CHECK (length(name) > 0),
	last_active_at			DATETIME			NOT NULL DEFAULT (datetime()),

	CONSTRAINT "PK.Workers_WorkerId" PRIMARY KEY (worker_id AUTOINCREMENT)
);

CREATE INDEX IF NOT EXISTS "IX.Workers_LastActiveAt" ON "workers" (last_active_at);

CREATE TRIGGER IF NOT EXISTS "TRG.RemoveInactiveWorkers"
AFTER INSERT ON "workers" FOR EACH ROW
BEGIN
	UPDATE "configurations" SET "active_worker_id" = NULL WHERE "active_worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < datetime('now', '-5 minutes')
	);

	DELETE FROM "workers" WHERE NOT EXISTS (
		SELECT * FROM "configurations" c WHERE c."active_worker_id" = "worker_id"
	) AND NOT EXISTS (
		SELECT * FROM "chunks" c WHERE c."worker_id" = "worker_id"
	) AND "last_active_at" < datetime('now', '-5 minutes');
END;

CREATE TABLE IF NOT EXISTS "configurations" (
	configuration_id		INTEGER				NOT NULL,

	active_worker_id		INTEGER					NULL,
	simulation_id			INTEGER				NOT NULL,
	metadata_id				INTEGER				NOT NULL,
	lattice_size			INTEGER				NOT NULL CHECK (lattice_size > 0),
	temperature				REAL				NOT NULL CHECK (temperature > 0.0),

	CONSTRAINT "PK.Configurations_ConfigurationId" PRIMARY KEY (configuration_id AUTOINCREMENT),
	CONSTRAINT "FK.Configurations_ActiveWorkerId" FOREIGN KEY (active_worker_id) REFERENCES "workers" (active_worker_id),
	CONSTRAINT "FK.Configurations_SimulationId"	FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id),
	CONSTRAINT "FK.Configurations_MetadataId" FOREIGN KEY (metadata_id) REFERENCES "metadata" (metadata_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Configurations_MetadataId_Algorithm_LatticeSize_Temperature" ON "configurations" (simulation_id, metadata_id, lattice_size, temperature);

CREATE INDEX IF NOT EXISTS "IX.Configurations_ActiveWorkerId" ON "configurations" (active_worker_id);

CREATE TRIGGER IF NOT EXISTS "TRG.UpdateWorkerLastActive"
AFTER UPDATE OF "active_worker_id" ON "configurations"
FOR EACH ROW WHEN NEW."active_worker_id" IS NOT NULL
BEGIN
	UPDATE "workers" SET "last_active_at" = datetime() WHERE "worker_id" = NEW."active_worker_id";
END;


CREATE TABLE IF NOT EXISTS "types" (
	type_id					INTEGER				NOT NULL,

	name					TEXT				NOT NULL,

	CONSTRAINT "PK.Types_TypeId" PRIMARY KEY (type_id)
);

INSERT OR IGNORE INTO "types" (type_id, name) VALUES (0, 'Energy'), (1, 'Energy Squared'), (2, 'Specific Heat'), (3, 'Magnetization'), (4, 'Magnetization Squared'), (5, 'Magnetic Susceptibility');


CREATE TABLE IF NOT EXISTS "chunks" (
	chunk_id				INTEGER				NOT NULL,

	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	worker_id				INTEGER				NOT NULL,
	spins					BLOB				NOT NULL,

	CONSTRAINT "PK.Chunks_ChunkId" PRIMARY KEY (chunk_id AUTOINCREMENT),
	CONSTRAINT "FK.Chunks_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Chunks_WorkerId" FOREIGN KEY (worker_id) REFERENCES "workers" (worker_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Chunks_ConfigurationId_Index" ON "chunks" ("configuration_id", "index");

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

constexpr std::string_view RegisterWorkerQuery = R"~~~~~~(
INSERT INTO "workers" (name) VALUES (@name) RETURNING "worker_id"
)~~~~~~";

SQLiteStorage::SQLiteStorage(const std::string_view & path) : worker_id(-1), db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE | SQLite::OPEN_FULLMUTEX) {
	try {
		db.setBusyTimeout(10000);
		db.exec("PRAGMA synchronous = NORMAL");

		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };
		db.exec(Migrations.data());

		SQLite::Statement worker { db, RegisterWorkerQuery.data() };
		worker.bind("@name", utils::hostname());

		while (worker.executeStep()) {
			worker_id = worker.getColumn(0).getInt();
		}

		transaction.commit();
	} catch (const std::exception &e) {
		std::cout << "Failed to migrate database. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertSimulationQuery = R"~~~~~~(
INSERT INTO "simulations" (simulation_id, bootstrap_resamples) VALUES (@simulation_id, @bootstrap_resamples)
ON CONFLICT DO UPDATE SET bootstrap_resamples = @bootstrap_resamples
)~~~~~~";

constexpr std::string_view InsertMetadataQuery = R"~~~~~~(
INSERT INTO "metadata" (simulation_id, algorithm, num_chunks, sweeps_per_chunk) VALUES (@simulation_id, @algorithm, @num_chunks, @sweeps_per_chunk)
ON CONFLICT DO UPDATE SET num_chunks = @num_chunks WHERE num_chunks < @num_chunks RETURNING metadata_id
)~~~~~~";

constexpr std::string_view InsertConfigurationsQuery = R"~~~~~~(
INSERT INTO "configurations" (simulation_id, metadata_id, lattice_size, temperature) VALUES (@simulation_id, @metadata_id, @lattice_size, @temperature)
ON CONFLICT DO NOTHING
)~~~~~~";

void SQLiteStorage::prepare_simulation(const Config config) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement simulation { db, InsertSimulationQuery.data() };
		simulation.bind("@simulation_id", static_cast<int>(config.simulation_id));
		simulation.bind("@bootstrap_resamples", static_cast<int>(config.bootstrap_resamples));
		simulation.exec();

		SQLite::Statement metadata { db, InsertMetadataQuery.data() };
		SQLite::Statement configurations { db, InsertConfigurationsQuery.data() };

		for (const auto & [key, value] : config.algorithms) {
			metadata.bind("@simulation_id", static_cast<int>(config.simulation_id));
			metadata.bind("@algorithm", key);
			metadata.bind("@num_chunks", static_cast<int>(value.num_chunks));
			metadata.bind("@sweeps_per_chunk", static_cast<int>(value.sweeps_per_chunk));

			const auto metadata_id = metadata.executeStep() ? metadata.getColumn(0).getInt() : -1;
			metadata.reset();

			for (const auto size : value.sizes) {
				for (const auto temperature : utils::sweep_through_temperature(config.max_temperature, config.temperature_steps)) {
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
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view NextChunkQuery = R"~~~~~~(
SELECT c.configuration_id, IfNull(k.num_chunks, 0) + 1, m.algorithm, c.lattice_size, c.temperature, m.sweeps_per_chunk, json(k.spins)
FROM simulations s
INNER JOIN configurations c on s.simulation_id = c.simulation_id
INNER JOIN metadata m ON c.metadata_id = m.metadata_id
LEFT JOIN (
    SELECT k.configuration_id, k.spins, MAX(k."index") AS num_chunks
    FROM chunks k
	GROUP BY k.configuration_id
) k ON c.configuration_id = k.configuration_id
WHERE s.simulation_id = @simulation_id AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
	SELECT "worker_id" FROM "workers" WHERE "last_active_at" < datetime('now', '-5 minutes')
)) AND IfNull(k.num_chunks, 0) + 1 < m.num_chunks
ORDER BY c.lattice_size DESC LIMIT 1;
)~~~~~~";

constexpr std::string_view SetConfigurationActiveWorker = R"~~~~~~(
UPDATE "configurations" SET "active_worker_id" = @worker_id WHERE "configuration_id" = @configuration_id;
)~~~~~~";

std::optional<Chunk> SQLiteStorage::next_chunk(const int simulation_id) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement next_chunk { db, NextChunkQuery.data() };
		next_chunk.bind("@simulation_id", simulation_id);

		if (!next_chunk.executeStep()) return std::nullopt;
		const auto configuration_id = next_chunk.getColumn(0).getInt();

		SQLite::Statement worker { db, SetConfigurationActiveWorker.data() };
		worker.bind("@configuration_id", configuration_id);
		worker.bind("@worker_id", worker_id);

		if (worker.exec() != 1) return std::nullopt;
		transaction.commit();

		std::optional<std::vector<double>> spins = std::nullopt;
		if (!next_chunk.getColumn(6).isNull()) {
			const auto str = next_chunk.getColumn(6).getString();
			const auto json = nlohmann::json::parse(str);
			spins = std::optional(json.get<std::vector<double>>());
		}

		return std::optional<Chunk>({
			next_chunk.getColumn(0).getInt(),
			next_chunk.getColumn(1).getInt(),
			static_cast<algorithms::Algorithm>(next_chunk.getColumn(2).getInt()),
			static_cast<std::size_t>(next_chunk.getColumn(3).getInt()),
			next_chunk.getColumn(4).getDouble(),
			static_cast<std::size_t>(next_chunk.getColumn(5).getInt()),
			spins
		});

	} catch (std::exception & e) {
		std::cout << "Failed to fetch next chunk. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertChunkQuery = R"~~~~~~(
INSERT INTO "chunks" (configuration_id, "index", worker_id, spins) VALUES (@configuration_id, @chunk_id, @worker_id, jsonb(@spins)) RETURNING chunk_id
)~~~~~~";

constexpr std::string_view InsertResultQuery = R"~~~~~~(
INSERT INTO "results" (chunk_id, type_id, "index", value) VALUES (@chunk_id, @type_id, @index, @value)
)~~~~~~";

constexpr std::string_view RemoveWorkerQuery = R"~~~~~~(
UPDATE "configurations" SET active_worker_id = NULL WHERE "configuration_id" = @configuration_id AND "active_worker_id" = @worker_id
)~~~~~~";

void SQLiteStorage::save_chunk(const Chunk & chunk, [[maybe_unused]] const std::vector<double> & spins, const observables::Map & results) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement chunk_stmt { db, InsertChunkQuery.data() };
		chunk_stmt.bind("@configuration_id", chunk.configuration_id);
		chunk_stmt.bind("@chunk_id", chunk.chunk_id);
		chunk_stmt.bind("@spins", nlohmann::json { spins }.at(0).dump());
		chunk_stmt.bind("@worker_id", worker_id);

		auto chunk_id = -1;
		while (chunk_stmt.executeStep()) chunk_id = chunk_stmt.getColumn(0).getInt();

		SQLite::Statement result { db, InsertResultQuery.data() };
		for (const auto & [key, measurements] : results) {
			break;
			for (std::size_t i = 0; i < measurements.size(); ++i) {
				result.bind("@chunk_id", chunk_id);
				result.bind("@type_id", key);
				result.bind("@index", static_cast<int>(i));
				result.bind("@value", measurements[i]);

				result.exec();
				result.reset();
			}
		}

		SQLite::Statement worker_stmt { db, RemoveWorkerQuery.data() };
		worker_stmt.bind("@configuration_id", chunk.configuration_id);
		worker_stmt.bind("@worker_id", worker_id);
		worker_stmt.exec();

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "Failed to save chunk. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}


constexpr std::string_view UpdateWorkerLastActive = R"~~~~~~(
UPDATE "workers" SET "last_active_at" = datetime() WHERE "worker_id" = @worker_id;
)~~~~~~";

void SQLiteStorage::worker_keep_alive() {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement worker { db, UpdateWorkerLastActive.data() };
		worker.bind("@worker_id", worker_id);

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "Failed to send worker keep alive. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

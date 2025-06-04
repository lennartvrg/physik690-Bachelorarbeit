#include <nlohmann/json.hpp>
#include <iostream>
#include <SQLiteCpp/Transaction.h>

#include "utils/utils.hpp"
#include "storage/sqlite_storage.hpp"

#include "schemas/measurements_generated.h"
#include "schemas/spins_generated.h"

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
	CONSTRAINT "FK.Configurations_ActiveWorkerId" FOREIGN KEY (active_worker_id) REFERENCES "workers" (worker_id),
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

INSERT OR IGNORE INTO "types" (type_id, name) VALUES (0, 'Energy'), (1, 'Energy Squared'), (2, 'Magnetization'), (3, 'Magnetization Squared'), (4, 'Specific Heat'), (5, 'Magnetic Susceptibility');


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

	tau						REAL				NOT NULL,
	data					BLOB				NOT NULL,

	CONSTRAINT "PK.Results_ChunkId_TypeId_Index" PRIMARY KEY (chunk_id, type_id),
	CONSTRAINT "FK.Results_ChunkId" FOREIGN KEY (chunk_id) REFERENCES "chunks" (chunk_id),
	CONSTRAINT "FK.Results_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
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
		db.exec("PRAGMA foreign_keys = ON");
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
ON CONFLICT DO UPDATE SET num_chunks = @num_chunks WHERE num_chunks < @num_chunks
)~~~~~~";

constexpr std::string_view FetchMetadataQuery = R"~~~~~~(
SELECT metadata_id FROM "metadata" WHERE simulation_id = @simulation_id AND algorithm = @algorithm
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

		SQLite::Statement insert_metadata { db, InsertMetadataQuery.data() };
		SQLite::Statement fetch_metadata { db, FetchMetadataQuery.data() };
		SQLite::Statement configurations { db, InsertConfigurationsQuery.data() };

		for (const auto & [key, value] : config.algorithms) {
			insert_metadata.bind("@simulation_id", static_cast<int>(config.simulation_id));
			insert_metadata.bind("@algorithm", key);
			insert_metadata.bind("@num_chunks", static_cast<int>(value.num_chunks));
			insert_metadata.bind("@sweeps_per_chunk", static_cast<int>(value.sweeps_per_chunk));

			insert_metadata.exec();
			insert_metadata.reset();

			fetch_metadata.bind("@simulation_id", static_cast<int>(config.simulation_id));
			fetch_metadata.bind("@algorithm", key);

			auto metadata_id = fetch_metadata.executeStep() ? fetch_metadata.getColumn(0).getInt() : -1;
			fetch_metadata.reset();

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
SELECT c.configuration_id, IfNull(k.num_chunks, 0) + 1, m.algorithm, c.lattice_size, c.temperature, m.sweeps_per_chunk, k.spins
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
)) AND IfNull(k.num_chunks, 0) < m.num_chunks
ORDER BY IfNull(k.num_chunks, 0) ASC LIMIT 1;
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
			const auto buffer = next_chunk.getColumn(6).getBlob();
			const auto data = schemas::GetSpins(buffer);
			spins = std::vector(data->data()->begin(), data->data()->end());
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
INSERT INTO "chunks" (configuration_id, "index", worker_id, spins) VALUES (@configuration_id, @index, @worker_id, @spins) RETURNING chunk_id
)~~~~~~";

constexpr std::string_view InsertResultQuery = R"~~~~~~(
INSERT INTO "results" (chunk_id, type_id, tau, data) VALUES (@chunk_id, @type_id, @tau, @data)
)~~~~~~";

constexpr std::string_view RemoveWorkerQuery = R"~~~~~~(
UPDATE "configurations" SET active_worker_id = NULL WHERE "configuration_id" = @configuration_id AND "active_worker_id" = @worker_id
)~~~~~~";

void SQLiteStorage::save_chunk(const Chunk & chunk, const std::span<uint8_t> & spins, const std::map<observables::Type, std::tuple<double, std::span<uint8_t>>> & results) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement chunk_stmt { db, InsertChunkQuery.data() };
		chunk_stmt.bind("@configuration_id", chunk.configuration_id);
		chunk_stmt.bind("@index", chunk.index);
		chunk_stmt.bind("@worker_id", worker_id);
		chunk_stmt.bind("@spins", spins.data(), static_cast<int>(spins.size()));

		auto chunk_id = -1;
		while (chunk_stmt.executeStep()) chunk_id = chunk_stmt.getColumn(0).getInt();

		SQLite::Statement result { db, InsertResultQuery.data() };
		for (const auto & [key, value] : results) {
			const auto [tau, data] = value;
			result.bind("@chunk_id", chunk_id);
			result.bind("@type_id", key);
			result.bind("@tau", tau);
			result.bind("@data", data.data(), static_cast<int>(data.size()));

			result.exec();
			result.reset();
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

constexpr std::string_view FetchNextEstimateQuery = R"~~~~~~(
SELECT c.configuration_id, s.bootstrap_resamples, m.num_chunks, t.type_id
FROM "simulations" s
INNER JOIN "configurations" c ON c.simulation_id = s.simulation_id
INNER JOIN "metadata" m ON c.metadata_id = m.metadata_id
CROSS JOIN "types" t ON t.type_id BETWEEN 0 AND 3
LEFT JOIN "estimates" e ON e.configuration_id = c.configuration_id AND e.type_id = t.type_id
WHERE s.simulation_id = @simulation_id AND c.active_worker_id IS NULL AND e.type_id IS NULL
LIMIT 1
)~~~~~~";

constexpr std::string_view FetchConfigurationResults = R"~~~~~~(
SELECT c."index", r.data
FROM "chunks" c
INNER JOIN "results" r ON c.chunk_id = r.chunk_id AND r.type_id = @type_id
WHERE c.configuration_id = @configuration_id
ORDER BY c."index"
)~~~~~~";

std::optional<std::tuple<Estimate, std::vector<double>>> SQLiteStorage::next_estimate(const int simulation_id) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement estimate_stmt { db, FetchNextEstimateQuery.data() };
		estimate_stmt.bind("@simulation_id", simulation_id);

		if (!estimate_stmt.executeStep()) return std::nullopt;
		const auto configuration_id = estimate_stmt.getColumn(0).getInt();
		const auto bootstrap_resamples = static_cast<std::size_t>(estimate_stmt.getColumn(1).getInt());
		const auto num_chunks = estimate_stmt.getColumn(2).getInt();
		const auto type = static_cast<observables::Type>(estimate_stmt.getColumn(3).getInt());

		SQLite::Statement worker { db, SetConfigurationActiveWorker.data() };
		worker.bind("@configuration_id", configuration_id);
		worker.bind("@worker_id", worker_id);
		if (worker.exec() != 1) return std::nullopt;

		SQLite::Statement result_stmt { db, FetchConfigurationResults.data() };
		result_stmt.bind("@configuration_id", configuration_id);
		result_stmt.bind("@type_id", type);

		std::vector<double> values {};
		while (result_stmt.executeStep()) {
			const auto buffer = result_stmt.getColumn(1).getBlob();
			const auto data = schemas::GetMeasurements(buffer);

			values.reserve(data->data()->size() * num_chunks);
			values.insert(values.end(), data->data()->begin(), data->data()->end());
		}
		transaction.commit();

		if (values.empty()) {
			std::cout << "No results found for " << configuration_id << " type " << type << std::endl;
		}

		return std::optional(std::make_tuple<Estimate, std::vector<double>>({ configuration_id, type, bootstrap_resamples }, std::move(values)));
	} catch (std::exception & e) {
		std::cout << "Failed to fetch next estimate. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertEstimateQuery = R"~~~~~~(
INSERT INTO "estimates" (configuration_id, type_id, mean, std_dev) VALUES (@configuration_id, @type_id, @mean, @std_dev)
)~~~~~~";

void SQLiteStorage::save_estimate(const int configuration_id, const observables::Type type, const double mean, const double std_dev) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement estimate_stmt { db, InsertEstimateQuery.data() };
		estimate_stmt.bind("@configuration_id", configuration_id);
		estimate_stmt.bind("@type_id", type);
		estimate_stmt.bind("@mean", mean);
		estimate_stmt.bind("@std_dev", std_dev);
		estimate_stmt.exec();

		SQLite::Statement worker_stmt { db, RemoveWorkerQuery.data() };
		worker_stmt.bind("@configuration_id", configuration_id);
		worker_stmt.bind("@worker_id", worker_id);
		worker_stmt.exec();

		transaction.commit();
	} catch (const std::exception & e) {
		std::cout << "Failed to save estimate. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view FetchNextDerivativeQuery = R"~~~~~~(
SELECT e.configuration_id, e.type_id, c.temperature, e.mean, o.mean
FROM "estimates" e
INNER JOIN "configurations" c ON e.configuration_id = c.configuration_id AND c.simulation_id = @simulation_id AND c.active_worker_id IS NULL
INNER JOIN "estimates" o ON e.configuration_id = o.configuration_id AND o.type_id = CASE WHEN e.type_id BETWEEN 0 AND 1 THEN mod(e.type_id + 1, 2) ELSE mod(e.type_id - 1, 2) + 2 END
LEFT JOIN "estimates" t ON e.configuration_id = t.configuration_id AND t.type_id = CASE WHEN e.type_id BETWEEN 0 AND 1 THEN 4 ELSE 5 END
WHERE (e.type_id BETWEEN 0 AND 3) AND (t.configuration_id IS NULL)
LIMIT 1
)~~~~~~";

std::optional<NextDerivative> SQLiteStorage::next_derivative(const int simulation_id) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement next_derivative_stmt { db, FetchNextDerivativeQuery.data() };
		next_derivative_stmt.bind("@simulation_id", simulation_id);

		if (!next_derivative_stmt.executeStep()) return std::nullopt;
		const auto configuration_id = next_derivative_stmt.getColumn(0).getInt();
		const auto type = static_cast<observables::Type>(next_derivative_stmt.getColumn(1).getInt());
		const auto temperature = next_derivative_stmt.getColumn(2).getDouble();
		const auto mean = next_derivative_stmt.getColumn(3).getDouble();
		const auto sqr_mean = next_derivative_stmt.getColumn(4).getDouble();

		SQLite::Statement worker { db, SetConfigurationActiveWorker.data() };
		worker.bind("@configuration_id", configuration_id);
		worker.bind("@worker_id", worker_id);

		if (worker.exec() != 1) return std::nullopt;
		transaction.commit();

		return { { configuration_id, type, temperature, mean, sqr_mean } };
	} catch (const std::exception & e) {
		std::cout << "Failed to fetch next derivative. SQLite exception: " << e.what() << std::endl;
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

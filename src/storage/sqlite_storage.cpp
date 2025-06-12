#include <nlohmann/json.hpp>
#include <iostream>
#include <SQLiteCpp/Transaction.h>

#include "utils/utils.hpp"
#include "storage/sqlite_storage.hpp"
#include "migrations.cpp"
#include "schemas/serialize.hpp"

constexpr std::string_view SQLITE_MIGRATIONS = R"~~~~~~(
CREATE TRIGGER IF NOT EXISTS "TRG.RemoveInactiveWorkers"
AFTER INSERT ON "workers" FOR EACH ROW
BEGIN
	UPDATE "configurations" SET "active_worker_id" = NULL WHERE "active_worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
	);

	DELETE FROM "workers" WHERE NOT EXISTS (
		SELECT * FROM "configurations" c WHERE c."active_worker_id" = "worker_id"
	) AND NOT EXISTS (
		SELECT * FROM "chunks" c WHERE c."worker_id" = "worker_id"
	) AND "last_active_at" < unixepoch('now', '-5 minutes');
END;


CREATE TRIGGER IF NOT EXISTS "TRG.UpdateWorkerLastActive"
AFTER UPDATE OF "active_worker_id" ON "configurations"
FOR EACH ROW WHEN NEW."active_worker_id" IS NOT NULL
BEGIN
	UPDATE "workers" SET "last_active_at" = unixepoch() WHERE "worker_id" = NEW."active_worker_id";
END;

CREATE TABLE IF NOT EXISTS "autocorrelation" (
	autocorrelation_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	data					BYTEA				NOT NULL,

	CONSTRAINT "PK.Autocorrelation_AutocorrelationId_TypeId" PRIMARY KEY (autocorrelation_id, type_id),
	CONSTRAINT "FK.Autocorrelation_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);


CREATE TABLE IF NOT EXISTS "chunks" (
	chunk_id				INTEGER				NOT NULL,

	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	autocorrelation_id		INTEGER					NULL CHECK ((autocorrelation_id != NULL AND "index" = 0) OR (autocorrelation_id = NULL AND "index" != 0)),
	worker_id				INTEGER				NOT NULL,

	start_time				INTEGER				NOT NULL,
	end_time				INTEGER				NOT NULL CHECK (end_time >= start_time),
	time					INTEGER				GENERATED ALWAYS AS (end_time - start_time),

	spins					BLOB				NOT NULL,

	CONSTRAINT "PK.Chunks_ChunkId" PRIMARY KEY (chunk_id),
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
)~~~~~~";

constexpr std::string_view RegisterWorkerQuery = R"~~~~~~(
INSERT INTO "workers" (name, last_active_at) VALUES (@name, @last_active_at) RETURNING "worker_id"
)~~~~~~";

SQLiteStorage::SQLiteStorage(const std::string_view & path) : worker_id(-1), db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE | SQLite::OPEN_FULLMUTEX) {
	try {
		db.setBusyTimeout(10000);
		db.exec("PRAGMA foreign_keys = ON");
		db.exec("PRAGMA synchronous = NORMAL");

		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };
		db.exec(MIGRATIONS.data());
		db.exec(SQLITE_MIGRATIONS.data());

		SQLite::Statement worker { db, RegisterWorkerQuery.data() };
		worker.bind("@name", utils::hostname());
		worker.bind("@last_active_at", utils::timestamp_ms());

		if (!worker.executeStep()) throw std::invalid_argument("Did not insert worker!");
		worker_id = worker.getColumn(0).getInt();

		while (worker.executeStep()) { }
		transaction.commit();
	} catch (const std::exception &e) {
		std::cout << "[SQLite] Failed to migrate database. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertSimulationQuery = R"~~~~~~(
INSERT INTO "simulations" (simulation_id, bootstrap_resamples, created_at) VALUES (@simulation_id, @bootstrap_resamples, @created_at)
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
INSERT INTO "configurations" (simulation_id, metadata_id, lattice_size, temperature_numerator, temperature_denominator) VALUES (@simulation_id, @metadata_id, @lattice_size, @temperature_numerator, @temperature_denominator)
ON CONFLICT DO NOTHING
)~~~~~~";

void SQLiteStorage::prepare_simulation(const Config config) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement simulation { db, InsertSimulationQuery.data() };
		simulation.bind("@simulation_id", config.simulation_id);
		simulation.bind("@bootstrap_resamples", static_cast<int>(config.bootstrap_resamples));
		simulation.bind("@created_at", utils::timestamp_ms());
		simulation.exec();

		SQLite::Statement insert_metadata { db, InsertMetadataQuery.data() };
		SQLite::Statement fetch_metadata { db, FetchMetadataQuery.data() };
		SQLite::Statement configurations { db, InsertConfigurationsQuery.data() };

		for (const auto & [key, value] : config.algorithms) {
			insert_metadata.bind("@simulation_id", config.simulation_id);
			insert_metadata.bind("@algorithm", key);
			insert_metadata.bind("@num_chunks", static_cast<int>(value.num_chunks));
			insert_metadata.bind("@sweeps_per_chunk", static_cast<int>(value.sweeps_per_chunk));

			insert_metadata.exec();
			insert_metadata.reset();

			fetch_metadata.bind("@simulation_id", config.simulation_id);
			fetch_metadata.bind("@algorithm", key);

			if (!fetch_metadata.executeStep()) throw std::invalid_argument("Did not insert metadata");
			const auto metadata_id = fetch_metadata.getColumn(0).getInt();

			while (fetch_metadata.executeStep()) { }
			fetch_metadata.reset();

			for (const auto size : value.sizes) {
				for (const auto temperature : utils::sweep_through_temperature(config.max_temperature, config.temperature_steps)) {
					configurations.bind("@simulation_id", config.simulation_id);
					configurations.bind("@metadata_id", metadata_id);
					configurations.bind("@lattice_size", static_cast<int>(size));
					configurations.bind("@temperature_numerator", temperature.numerator);
					configurations.bind("@temperature_denominator", temperature.denominator);

					configurations.exec();
					configurations.reset();
				}
			}
		}

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to prepare simulation. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view NextChunkQuery = R"~~~~~~(
SELECT c.configuration_id, IfNull(k.num_chunks, 0) + 1, m.algorithm, c.lattice_size, c.temperature_numerator, c.temperature_denominator, m.sweeps_per_chunk, k.spins
FROM simulations s
INNER JOIN configurations c on s.simulation_id = c.simulation_id
INNER JOIN metadata m ON c.metadata_id = m.metadata_id
LEFT JOIN (
    SELECT k.configuration_id, k.spins, MAX(k."index") AS num_chunks
    FROM chunks k
	GROUP BY k.configuration_id
) k ON c.configuration_id = k.configuration_id
WHERE s.simulation_id = @simulation_id AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
	SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
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

		std::optional<std::vector<double_t>> spins = std::nullopt;
		if (!next_chunk.getColumn(7).isNull()) {
			const auto buffer = next_chunk.getColumn(7).getBlob();
			spins = schemas::deserialize(buffer);
		}

		return std::optional<Chunk>({
			next_chunk.getColumn(0).getInt(),
			next_chunk.getColumn(1).getInt(),
			static_cast<algorithms::Algorithm>(next_chunk.getColumn(2).getInt()),
			static_cast<std::size_t>(next_chunk.getColumn(3).getInt()),
			utils::ratio { next_chunk.getColumn(4).getInt(), next_chunk.getColumn(5).getInt() },
			static_cast<std::size_t>(next_chunk.getColumn(6).getInt()),
			spins
		});

	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to fetch next chunk. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertChunkQuery = R"~~~~~~(
INSERT INTO "chunks" (configuration_id, "index", start_time, end_time, worker_id, spins) VALUES (@configuration_id, @index, @start_time, @end_time, @worker_id, @spins) RETURNING chunk_id
)~~~~~~";

constexpr std::string_view InsertResultQuery = R"~~~~~~(
INSERT INTO "results" (chunk_id, type_id, tau, data) VALUES (@chunk_id, @type_id, @tau, @data)
)~~~~~~";

constexpr std::string_view RemoveWorkerQuery = R"~~~~~~(
UPDATE "configurations" SET active_worker_id = NULL WHERE "configuration_id" = @configuration_id AND "active_worker_id" = @worker_id
)~~~~~~";

void SQLiteStorage::save_chunk(const Chunk & chunk, const int64_t start_time, const int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>>> & results) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement chunk_stmt { db, InsertChunkQuery.data() };
		chunk_stmt.bind("@configuration_id", chunk.configuration_id);
		chunk_stmt.bind("@index", chunk.index);
		chunk_stmt.bind("@start_time", start_time);
		chunk_stmt.bind("@end_time", end_time);
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
		std::cout << "[SQLite] Failed to save chunk. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view FetchNextEstimateQuery = R"~~~~~~(
SELECT c.configuration_id, s.bootstrap_resamples, m.num_chunks, t.type_id
FROM "simulations" s
INNER JOIN "configurations" c ON c.simulation_id = s.simulation_id
INNER JOIN "metadata" m ON c.metadata_id = m.metadata_id
INNER JOIN "chunks" ch ON c.configuration_id = ch.configuration_id AND ch."index" = m.num_chunks
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

std::optional<std::tuple<Estimate, std::vector<double_t>>> SQLiteStorage::next_estimate(const int simulation_id) {
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

		std::vector<double_t> values {};
		while (result_stmt.executeStep()) {
			const auto buffer = result_stmt.getColumn(1).getBlob();
			const auto data = schemas::deserialize(buffer);

			values.reserve(data.size() * num_chunks);
			values.insert(values.end(), data.begin(), data.end());
		}
		transaction.commit();

		if (values.empty()) {
			std::cout << "No results found for " << configuration_id << " type " << type << std::endl;
		}

		return std::optional(std::make_tuple<Estimate, std::vector<double_t>>({ configuration_id, type, bootstrap_resamples }, std::move(values)));
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to fetch next estimate. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertEstimateQuery = R"~~~~~~(
INSERT INTO "estimates" (configuration_id, type_id, start_time, end_time, mean, std_dev) VALUES (@configuration_id, @type_id, @start_time, @end_time, @mean, @std_dev)
)~~~~~~";

void SQLiteStorage::save_estimate(const int configuration_id, const int64_t start_time, const int64_t end_time, const observables::Type type, const double_t mean, const double_t std_dev) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement estimate_stmt { db, InsertEstimateQuery.data() };
		estimate_stmt.bind("@configuration_id", configuration_id);
		estimate_stmt.bind("@type_id", type);
		estimate_stmt.bind("@start_time", start_time);
		estimate_stmt.bind("@end_time", end_time);
		estimate_stmt.bind("@mean", mean);
		estimate_stmt.bind("@std_dev", std_dev);
		estimate_stmt.exec();

		SQLite::Statement worker_stmt { db, RemoveWorkerQuery.data() };
		worker_stmt.bind("@configuration_id", configuration_id);
		worker_stmt.bind("@worker_id", worker_id);
		worker_stmt.exec();

		transaction.commit();
	} catch (const std::exception & e) {
		std::cout << "[SQLite] Failed to save estimate. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view FetchNextDerivativeQuery = R"~~~~~~(
SELECT e.configuration_id, e.type_id, c.temperature_numerator, c.temperature_denominator, e.mean, e.std_dev, o.mean, o.std_dev
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
		const auto temperature = utils::ratio { next_derivative_stmt.getColumn(2).getInt(), next_derivative_stmt.getColumn(3).getInt() };
		const auto mean = next_derivative_stmt.getColumn(4).getDouble();
		const auto std_dev = next_derivative_stmt.getColumn(5).getDouble();
		const auto square_mean = next_derivative_stmt.getColumn(6).getDouble();
		const auto square_std_dev = next_derivative_stmt.getColumn(7).getDouble();

		SQLite::Statement worker { db, SetConfigurationActiveWorker.data() };
		worker.bind("@configuration_id", configuration_id);
		worker.bind("@worker_id", worker_id);

		if (worker.exec() != 1) return std::nullopt;
		transaction.commit();

		return { { configuration_id, type, temperature, mean, std_dev, square_mean, square_std_dev } };
	} catch (const std::exception & e) {
		std::cout << "[SQLite] Failed to fetch next derivative. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view UpdateWorkerLastActive = R"~~~~~~(
UPDATE "workers" SET "last_active_at" = unixepoch() WHERE "worker_id" = @worker_id;
)~~~~~~";

void SQLiteStorage::worker_keep_alive() {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement worker { db, UpdateWorkerLastActive.data() };
		worker.bind("@worker_id", worker_id);

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to send worker keep alive. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

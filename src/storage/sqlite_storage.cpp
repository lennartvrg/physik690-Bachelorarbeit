#include <iostream>
#include <SQLiteCpp/Transaction.h>

#include "utils/utils.hpp"
#include "storage/sqlite_storage.hpp"

#include <thread>

#include "schemas/serialize.hpp"

constexpr std::string_view SQLITE_MIGRATIONS = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	bootstrap_resamples		INTEGER				NOT NULL DEFAULT (100000) CHECK (bootstrap_resamples > 0),
	created_at				BIGINT				NOT NULL,

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
);


CREATE TABLE IF NOT EXISTS "metadata" (
	metadata_id				INTEGER				NOT NULL,

	simulation_id			INTEGER				NOT NULL,
	algorithm				INTEGER				NOT NULL CHECK (algorithm = 0 OR algorithm = 1),
	num_chunks				INTEGER				NOT NULL DEFAULT (10) CHECK (num_chunks > 0),
	sweeps_per_chunk		INTEGER				NOT NULL DEFAULT (100000) CHECK (sweeps_per_chunk > 0),

	CONSTRAINT "PK.Metadata_SimulationId" PRIMARY KEY (metadata_id),
	CONSTRAINT "FK.Metadata_SimulationId" FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Metadata_SimulationId_Algorithm" ON "metadata" (simulation_id, algorithm);


CREATE TABLE IF NOT EXISTS "workers" (
	worker_id				INTEGER				NOT NULL,

	name					TEXT				NOT NULL CHECK (length(name) > 0),
	synchronize				INTEGER					NULL,
	last_active_at			BIGINT				NOT NULL,

	CONSTRAINT "PK.Workers_WorkerId" PRIMARY KEY (worker_id)
);

CREATE INDEX IF NOT EXISTS "IX.Workers_LastActiveAt" ON "workers" (last_active_at);


CREATE TABLE IF NOT EXISTS "configurations" (
	configuration_id		INTEGER				NOT NULL,

	active_worker_id		INTEGER					NULL,
	simulation_id			INTEGER				NOT NULL,
	metadata_id				INTEGER				NOT NULL,
	lattice_size			INTEGER				NOT NULL CHECK (lattice_size > 0),
	temperature				REAL				NOT NULL CHECK (temperature > 0.0),

	completed_chunks		INTEGER				NOT NULL DEFAULT (0),
	depth					INTEGER				NOT NULL,

	CONSTRAINT "PK.Configurations_ConfigurationId" PRIMARY KEY (configuration_id),
	CONSTRAINT "FK.Configurations_ActiveWorkerId" FOREIGN KEY (active_worker_id) REFERENCES "workers" (worker_id),
	CONSTRAINT "FK.Configurations_SimulationId"	FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id),
	CONSTRAINT "FK.Configurations_MetadataId" FOREIGN KEY (metadata_id) REFERENCES "metadata" (metadata_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Configurations_MetadataId_Algorithm_LatticeSize_Temperature" ON "configurations" (simulation_id, metadata_id, lattice_size, temperature);

CREATE INDEX IF NOT EXISTS "IX.Configurations_ActiveWorkerId" ON "configurations" (active_worker_id);


CREATE TABLE IF NOT EXISTS "types" (
	type_id					INTEGER				NOT NULL,

	name					TEXT				NOT NULL,

	CONSTRAINT "PK.Types_TypeId" PRIMARY KEY (type_id)
);

INSERT INTO "types" (type_id, name) VALUES (0, 'Energy'), (1, 'Energy Squared'), (2, 'Magnetization'), (3, 'Magnetization Squared'), (4, 'Specific Heat'), (5, 'Magnetic Susceptibility'), (6, 'Helicity Modulus Intermediate'), (7, 'Helicity Modulus'), (8, 'Cluster size')
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS "estimates" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	start_time				INTEGER				NOT NULL,
	end_time				INTEGER				NOT NULL CHECK (end_time >= start_time),
	time					INTEGER				GENERATED ALWAYS AS (end_time - start_time) STORED,

	mean					REAL				NOT NULL,
	std_dev					REAL				NOT NULL,

	CONSTRAINT "PK.Estimates_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "FK.Estimates_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Estimates_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);


CREATE TABLE IF NOT EXISTS "vortices" (
	vortex_id				INTEGER				NOT NULL,

	simulation_id			INTEGER				NOT NULL,
	lattice_size			INTEGER				NOT NULL CHECK (lattice_size > 0),

	worker_id				INTEGER					NULL,

	CONSTRAINT "PK.Vortices_VortexId" PRIMARY KEY (vortex_id),
	CONSTRAINT "FK.Vortices_ActiveWorkerId" FOREIGN KEY (worker_id) REFERENCES "workers" (worker_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Vortices_SimulationId_LatticeSize" ON "vortices" (simulation_id, lattice_size);

CREATE INDEX IF NOT EXISTS "IX.Vortices_ActiveWorkerId" ON "vortices" (worker_id);


CREATE TABLE IF NOT EXISTS "vortex_results" (
	vortex_id				INTEGER				NOT NULL,
	sweeps					INTEGER				NOT NULL,

	temperature				REAL				NOT NULL,
	spins					BLOB				NOT NULL,

	CONSTRAINT "PK.VortexResults_VortexId_Sweeps" PRIMARY KEY (vortex_id, sweeps)
);

CREATE TRIGGER IF NOT EXISTS "TRG.RemoveInactiveWorkers"
AFTER INSERT ON "workers" FOR EACH ROW
BEGIN
	UPDATE "configurations" SET "active_worker_id" = NULL WHERE "active_worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
	);

	UPDATE "vortices" SET "worker_id" = NULL WHERE "worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
	);

	DELETE FROM "workers" WHERE NOT EXISTS (
		SELECT * FROM "configurations" c WHERE c."active_worker_id" = "worker_id"
	) AND NOT EXISTS (
		SELECT * FROM "vortices" v WHERE v."worker_id" = "worker_id"
	) AND "last_active_at" < unixepoch('now', '-5 minutes');
END;

CREATE TABLE IF NOT EXISTS "chunks" (
	chunk_id				INTEGER				NOT NULL,

	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	start_time				INTEGER				NOT NULL,
	end_time				INTEGER				NOT NULL CHECK (end_time >= start_time),
	time					INTEGER				GENERATED ALWAYS AS (end_time - start_time),

	spins					BLOB				NOT NULL,

	CONSTRAINT "PK.Chunks_ChunkId" PRIMARY KEY (chunk_id),
	CONSTRAINT "FK.Chunks_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Chunks_ConfigurationId_Index" ON "chunks" ("configuration_id", "index");


CREATE TABLE IF NOT EXISTS "autocorrelations" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	chunk_id				INTEGER				NOT NULL,
	data					BLOB				NOT NULL,

	CONSTRAINT "PK.Autocorrelations_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "Fk.Autocorrelations_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Autocorrelations_ChunkId" FOREIGN KEY (chunk_id) REFERENCES "chunks" (chunk_id),
	CONSTRAINT "FK.Autocorrelations_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Autocorrelations_ChunkId_TypeId" ON "autocorrelations" ("chunk_id", "type_id");


CREATE TABLE IF NOT EXISTS "results" (
	chunk_id				INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	tau						REAL				NOT NULL,
	data					BLOB				NOT NULL,

	CONSTRAINT "PK.Results_ChunkId_TypeId_Index" PRIMARY KEY (chunk_id, type_id),
	CONSTRAINT "FK.Results_ChunkId" FOREIGN KEY (chunk_id) REFERENCES "chunks" (chunk_id),
	CONSTRAINT "FK.Results_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);

CREATE TRIGGER IF NOT EXISTS "TRG.RemoveOutdatedEstimates"
AFTER INSERT ON "results" FOR EACH ROW
BEGIN
	DELETE FROM "estimates" WHERE type_id = NEW.type_id AND configuration_id IN (
		SELECT configuration_id FROM "chunks" c WHERE c.chunk_id = NEW.chunk_id
	);
END;

CREATE TRIGGER IF NOT EXISTS "TRG.OnUpdatedBootstrapResamples"
AFTER UPDATE ON "simulations" FOR EACH ROW WHEN NEW.bootstrap_resamples != OLD.bootstrap_resamples
BEGIN
	DELETE FROM "estimates" WHERE "configuration_id" IN (
		SELECT "configuration_id" FROM "configurations" WHERE "simulation_id" = NEW."simulation_id"
	);
END;

CREATE TRIGGER IF NOT EXISTS "TRG.OnInsertedChunk"
BEFORE INSERT ON "chunks" FOR EACH ROW
BEGIN
	UPDATE configurations SET completed_chunks = NEW."index" WHERE configuration_id = NEW.configuration_id;
END;
)~~~~~~";

constexpr std::string_view RegisterWorkerQuery = R"~~~~~~(
INSERT INTO "workers" (name, last_active_at) VALUES (@name, unixepoch('now')) RETURNING "worker_id"
)~~~~~~";

SQLiteStorage::SQLiteStorage(const std::string_view & path) : worker_id(-1), db(path, SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE | SQLite::OPEN_NOMUTEX) {
	try {
		db.setBusyTimeout(30000);
		db.exec("PRAGMA foreign_keys = ON");
		db.exec("PRAGMA synchronous = NORMAL");

		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };
		db.exec(SQLITE_MIGRATIONS.data());

		SQLite::Statement worker { db, RegisterWorkerQuery.data() };
		worker.bind("@name", utils::hostname());

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
INSERT INTO "simulations" (simulation_id, bootstrap_resamples, created_at) VALUES (@simulation_id, @bootstrap_resamples, unixepoch('now'))
ON CONFLICT DO UPDATE SET bootstrap_resamples = @bootstrap_resamples
)~~~~~~";

constexpr std::string_view InsertVorticesQuery = R"~~~~~~(
INSERT INTO "vortices" (simulation_id, lattice_size) VALUES (@simulation_id, @lattice_size)
ON CONFLICT DO NOTHING
)~~~~~~";

constexpr std::string_view InsertMetadataQuery = R"~~~~~~(
INSERT INTO "metadata" (simulation_id, algorithm, num_chunks, sweeps_per_chunk) VALUES (@simulation_id, @algorithm, @num_chunks, @sweeps_per_chunk)
ON CONFLICT DO UPDATE SET num_chunks = @num_chunks WHERE num_chunks < @num_chunks
)~~~~~~";

constexpr std::string_view FetchMetadataQuery = R"~~~~~~(
SELECT metadata_id FROM "metadata" WHERE simulation_id = @simulation_id AND algorithm = @algorithm
)~~~~~~";

constexpr std::string_view InsertConfigurationsQuery = R"~~~~~~(
INSERT INTO "configurations" (simulation_id, metadata_id, lattice_size, temperature, depth) VALUES (@simulation_id, @metadata_id, @lattice_size, @temperature, @depth)
ON CONFLICT DO NOTHING
)~~~~~~";

constexpr std::string_view FetchAllWorkDoneQuery = R"~~~~~~(
SELECT COUNT(*) AS count
FROM configurations c
    INNER JOIN metadata m ON c.metadata_id = m.metadata_id
    LEFT JOIN (
        SELECT c.configuration_id, COUNT(*) AS "done_chunks"
        FROM configurations c INNER JOIN chunks c2 on c.configuration_id = c2.configuration_id
        WHERE c.simulation_id = @simulation_id
        GROUP BY c.configuration_id
    ) c2 ON c.configuration_id = c2.configuration_id
    LEFT JOIN (
        SELECT c.configuration_id, COUNT(*) AS "done_estimates"
        FROM configurations c INNER JOIN estimates e ON c.configuration_id = e.configuration_id
        WHERE c.simulation_id = @simulation_id
        GROUP BY c.configuration_id
    ) e ON c.configuration_id = e.configuration_id
WHERE c.simulation_id = @simulation_id AND (c2.configuration_id IS NULL OR e.configuration_id IS NULL OR c2.done_chunks < m.num_chunks OR e.done_estimates != CASE WHEN m.algorithm = 0 THEN 8 ELSE 9 END);
)~~~~~~";

constexpr std::string_view FetchMaxDepthQuery = R"~~~~~~(
SELECT MAX(c.depth) AS max_depth
FROM configurations c
WHERE c.simulation_id = @simulation_id AND c.metadata_id = @metadata_id AND c.lattice_size = @lattice_size;
)~~~~~~";

constexpr std::string_view FetchPeakMagneticSusceptibilityQuery = R"~~~~~~(
SELECT c.temperature, (c.temperature - LAG(c.temperature, 1) OVER (ORDER BY c.temperature)) AS diff
FROM configurations c
         INNER JOIN estimates e ON c.configuration_id = e.configuration_id AND e.type_id = 5
WHERE c.simulation_id = @simulation_id AND c.metadata_id = @metadata_id AND c.lattice_size = @lattice_size
ORDER BY e.mean DESC
LIMIT 1;
)~~~~~~";

bool SQLiteStorage::prepare_simulation(const Config config) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement simulation { db, InsertSimulationQuery.data() };
		simulation.bind("@simulation_id", config.simulation_id);
		simulation.bind("@bootstrap_resamples", static_cast<int>(config.bootstrap_resamples));
		simulation.exec();

		SQLite::Statement vortex { db, InsertVorticesQuery.data() };
		for (const auto size : config.vortex_sizes) {
			vortex.bind("@simulation_id", config.simulation_id);
			vortex.bind("@lattice_size", static_cast<int>(size));
			vortex.exec();
			vortex.reset();
		}

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
			fetch_metadata.reset();

			for (const auto size : value.sizes) {
				for (const auto temperature : utils::sweep_temperature(0.0, config.max_temperature, config.temperature_steps)) {
					configurations.bind("@simulation_id", config.simulation_id);
					configurations.bind("@metadata_id", metadata_id);
					configurations.bind("@lattice_size", static_cast<int>(size));
					configurations.bind("@temperature", temperature);
					configurations.bind("@depth", 1);

					configurations.exec();
					configurations.reset();
				}
			}
		}

		SQLite::Statement max_depth_query { db, FetchMaxDepthQuery.data() };
		SQLite::Statement peak_xs_query {db, FetchPeakMagneticSusceptibilityQuery.data() };
		SQLite::Statement all_work_done { db, FetchAllWorkDoneQuery.data() };
		all_work_done.bind("@simulation_id", config.simulation_id);

		// Check if all registered configurations are completely done
		if (all_work_done.executeStep() && all_work_done.getColumn(0).getInt() == 0) {
			for (const auto & [key, value] : config.algorithms) {
				// Find metadata row associated with algorithm
				fetch_metadata.bind("@simulation_id", config.simulation_id);
				fetch_metadata.bind("@algorithm", key);

				if (!fetch_metadata.executeStep()) throw std::invalid_argument("Did not insert metadata");
				const auto metadata_id = fetch_metadata.getColumn(0).getInt();
				fetch_metadata.reset();

				// Iterate over sizes
				for (const auto size : value.sizes) {
					// Check depth not yet reached
					max_depth_query.bind("@simulation_id", config.simulation_id);
					max_depth_query.bind("@metadata_id", metadata_id);
					max_depth_query.bind("@lattice_size", static_cast<int>(size));

					// Check depth not yet reached
					if (max_depth_query.executeStep() && max_depth_query.getColumn(0).getInt() < config.max_depth) {
						// Fetch the Xs peak by the temperature where it occurred and the space to neighboring data points
						peak_xs_query.bind("@simulation_id", config.simulation_id);
						peak_xs_query.bind("@metadata_id", metadata_id);
						peak_xs_query.bind("@lattice_size", static_cast<int>(size));

						if (peak_xs_query.executeStep()) {
							// Extract temperature where xs is max and step size
							const auto xs_temperature = peak_xs_query.getColumn(0).getDouble();
							const auto diff = peak_xs_query.getColumn(1).getDouble();

							// Determine new bounds around the temperature where xs is max
							const auto min_temperature = xs_temperature - 2.0 * diff;
							const auto max_temperature = xs_temperature + 2.0 * diff;

							for (const auto temperature : utils::sweep_temperature(min_temperature, max_temperature, config.temperature_steps, false)) {
								configurations.bind("@simulation_id", config.simulation_id);
								configurations.bind("@metadata_id", metadata_id);
								configurations.bind("@lattice_size", static_cast<int>(size));
								configurations.bind("@temperature", temperature);
								configurations.bind("@depth", max_depth_query.getColumn(0).getInt() + 1);

								configurations.exec();
								configurations.reset();
							}
						}
						peak_xs_query.reset();
					}
					max_depth_query.reset();

				}

			}
		}

		all_work_done.reset();
		all_work_done.bind("@simulation_id", config.simulation_id);

		const bool work = all_work_done.executeStep() && all_work_done.getColumn(0).getInt() != 0;
		transaction.commit();

		return work;
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to prepare simulation. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view NextVortexQuery = R"~~~~~~(
SELECT v."vortex_id", v."lattice_size" FROM "vortices" v WHERE v."simulation_id" = @simulation_id AND NOT EXISTS (
	SELECT * FROM "vortex_results" vr WHERE vr.vortex_id = v.vortex_id
) AND (
	v."worker_id" IS NULL OR v."worker_id" IN (SELECT w."worker_id" FROM "workers" w WHERE w."last_active_at" < unixepoch('now', '-5 minutes'))
)
)~~~~~~";

constexpr std::string_view SetVortexActiveWorkerId = R"~~~~~~(
UPDATE "vortices" SET "worker_id" = @worker_id WHERE "vortex_id" = @vortex_id;
)~~~~~~";

std::optional<std::tuple<std::size_t, std::size_t>> SQLiteStorage::next_vortex(const int simulation_id) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement next_vortex { db, NextVortexQuery.data() };
		next_vortex.bind("@simulation_id", simulation_id);

		if (!next_vortex.executeStep()) return std::nullopt;
		const auto vortex_id = next_vortex.getColumn(0).getInt();
		const auto lattice_size = next_vortex.getColumn(1).getInt();

		SQLite::Statement worker { db, SetVortexActiveWorkerId.data() };
		worker.bind("@vortex_id", vortex_id);
		worker.bind("@worker_id", worker_id);

		if (worker.exec() != 1) return std::nullopt;
		transaction.commit();

		return {{ static_cast<std::size_t>(vortex_id), static_cast<std::size_t>(lattice_size) }};
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to fetch next vortex. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertVortexResultsQuery = R"~~~~~~(
INSERT INTO "vortex_results" (vortex_id, sweeps, temperature, spins) VALUES (@vortex_id, @sweeps, @temperature, @spins)
)~~~~~~";

void SQLiteStorage::save_vortices(const std::size_t vortex_id, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>> results) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement save_stmt { db, InsertVortexResultsQuery.data() };
		for (const auto & [temperature, sweeps, spins] : results) {
			const auto data = schemas::serialize(spins);

			save_stmt.bind("@vortex_id", static_cast<int>(vortex_id));
			save_stmt.bind("@sweeps", static_cast<int>(sweeps));
			save_stmt.bind("@temperature", temperature);
			save_stmt.bind("@spins", data.data(), static_cast<int>(data.size()));

			save_stmt.exec();
			save_stmt.reset();
		}

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to fetch save vortex results. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view NextChunkQuery = R"~~~~~~(
SELECT c.configuration_id, IfNull(k.num_chunks, 0) + 1 AS "index", m.algorithm, c.lattice_size, c.temperature, m.sweeps_per_chunk, k.spins
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
ORDER BY IfNull(k.num_chunks, 0) ASC, c.lattice_size DESC LIMIT 1
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
		if (!next_chunk.getColumn(6).isNull()) {
			const auto buffer = next_chunk.getColumn(6).getBlob();
			spins = schemas::deserialize(buffer);
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
		std::cout << "[SQLite] Failed to fetch next chunk. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertChunkQuery = R"~~~~~~(
INSERT INTO "chunks" (configuration_id, "index", start_time, end_time, spins) VALUES (@configuration_id, @index, @start_time, @end_time, @spins) RETURNING chunk_id
)~~~~~~";

constexpr std::string_view InsertAutocorrelationQuery = R"~~~~~~(
INSERT INTO "autocorrelations" (configuration_id, type_id, chunk_id, data) VALUES (@configuration_id, @type_id, @chunk_id, @data)
)~~~~~~";

constexpr std::string_view InsertResultQuery = R"~~~~~~(
INSERT INTO "results" (chunk_id, type_id, tau, data) VALUES (@chunk_id, @type_id, @tau, @data)
)~~~~~~";

constexpr std::string_view RemoveWorkerQuery = R"~~~~~~(
UPDATE "configurations" SET active_worker_id = NULL WHERE "configuration_id" = @configuration_id AND "active_worker_id" = @worker_id
)~~~~~~";

void SQLiteStorage::save_chunk(const Chunk & chunk, const int64_t start_time, const int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> & results) {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement chunk_stmt { db, InsertChunkQuery.data() };
		chunk_stmt.bind("@configuration_id", chunk.configuration_id);
		chunk_stmt.bind("@index", chunk.index);
		chunk_stmt.bind("@start_time", start_time);
		chunk_stmt.bind("@end_time", end_time);
		chunk_stmt.bind("@spins", spins.data(), static_cast<int>(spins.size()));

		auto chunk_id = -1;
		while (chunk_stmt.executeStep()) chunk_id = chunk_stmt.getColumn(0).getInt();

		SQLite::Statement result { db, InsertResultQuery.data() };
		SQLite::Statement autocorrelation_stmt { db, InsertAutocorrelationQuery.data() };

		for (const auto & [key, value] : results) {
			const auto [tau, values, autocorrelation_opt] = value;
			result.bind("@chunk_id", chunk_id);
			result.bind("@type_id", key);
			result.bind("@tau", tau);
			result.bind("@data", values.data(), static_cast<int>(values.size()));
			result.exec();
			result.reset();

			if (const auto autocorrelation = autocorrelation_opt) {
				autocorrelation_stmt.bind("@configuration_id", chunk.configuration_id);
				autocorrelation_stmt.bind("@type_id", key);
				autocorrelation_stmt.bind("@chunk_id", chunk_id);
				autocorrelation_stmt.bind("@data", autocorrelation->data(), static_cast<int>(autocorrelation->size()));
				autocorrelation_stmt.exec();
				autocorrelation_stmt.reset();
			}
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
CROSS JOIN "types" t ON t.type_id BETWEEN 0 AND 3 OR t.type_id = 6 OR (t.type_id = 8 AND m.algorithm = 1)
LEFT JOIN "estimates" e ON e.configuration_id = c.configuration_id AND e.type_id = t.type_id
WHERE s.simulation_id = @simulation_id AND c.completed_chunks = m.num_chunks AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
	SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
)) AND e.type_id IS NULL
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

		return { std::make_tuple<Estimate, std::vector<double_t>>({ configuration_id, type, bootstrap_resamples }, std::move(values)) };
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
SELECT e.configuration_id, e.type_id, c.temperature, e.mean, e.std_dev, o.mean, o.std_dev
FROM "estimates" e
INNER JOIN "configurations" c ON e.configuration_id = c.configuration_id AND c.simulation_id = @simulation_id
INNER JOIN "estimates" o ON e.configuration_id = o.configuration_id AND o.type_id = CASE WHEN e.type_id = 0 THEN 1 WHEN e.type_id = 2 THEN 3 ELSE 0 END
LEFT JOIN "estimates" t ON e.configuration_id = t.configuration_id AND t.type_id = CASE WHEN e.type_id = 0 THEN 4 WHEN e.type_id = 2 THEN 5 ELSE 7 END
WHERE (e.type_id = 0 OR e.type_id = 2 OR e.type_id = 6) AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
	SELECT "worker_id" FROM "workers" WHERE "last_active_at" < unixepoch('now', '-5 minutes')
)) AND (t.configuration_id IS NULL)
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
		const auto std_dev = next_derivative_stmt.getColumn(4).getDouble();
		const auto square_mean = next_derivative_stmt.getColumn(5).getDouble();
		const auto square_std_dev = next_derivative_stmt.getColumn(6).getDouble();

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
UPDATE "workers" SET "last_active_at" = unixepoch('now') WHERE "worker_id" = @worker_id;
)~~~~~~";

void SQLiteStorage::worker_keep_alive() {
	try {
		SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement worker { db, UpdateWorkerLastActive.data() };
		worker.bind("@worker_id", worker_id);
		worker.exec();

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to send worker keep alive. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view SetWorkerSynchronizeFlag = R"~~~~~~(
UPDATE "workers" SET "synchronize" = @synchronize WHERE "worker_id" = @worker_id
)~~~~~~";

constexpr std::string_view FetchWorkerSynchronizeFlag = R"~~~~~~(
SELECT "synchronize" FROM "workers" WHERE "worker_id" = @worker_id
)~~~~~~";

constexpr std::string_view CountActiveWorkerNotWaitingForSynchronization = R"~~~~~~(
SELECT COUNT(*) FROM "workers" WHERE "last_active_at" >= unixepoch('now', '-5 minutes') AND "synchronize" IS NULL
)~~~~~~";

constexpr std::string_view ResetWaitingFlag = R"~~~~~~(
UPDATE "workers" SET "synchronize" = NULL WHERE "last_active_at" >= unixepoch('now', '-5 minutes')
)~~~~~~";

void SQLiteStorage::synchronize_workers() {
	try {
		SQLite::Transaction init { db, SQLite::TransactionBehavior::IMMEDIATE };

		SQLite::Statement worker { db, SetWorkerSynchronizeFlag.data() };
		worker.bind("@worker_id", worker_id);
		worker.bind("@synchronize", 1);
		worker.exec();

		init.commit();
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to set worker waiting for synchronization. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}

	try {
		while (true) {
			SQLite::Transaction transaction { db, SQLite::TransactionBehavior::IMMEDIATE };

			SQLite::Statement synchronize_stmt { db, FetchWorkerSynchronizeFlag.data() };
			synchronize_stmt.bind("@worker_id", worker_id);

			if (synchronize_stmt.executeStep() && synchronize_stmt.getColumn(0).isNull()) {
				break;
			}

			SQLite::Statement not_waiting_stmt { db, CountActiveWorkerNotWaitingForSynchronization.data() };
			if (not_waiting_stmt.executeStep() && not_waiting_stmt.getColumn(0).getInt() == 0) {
				SQLite::Statement reset_stmt { db, ResetWaitingFlag.data() };
				reset_stmt.exec();
			}

			SQLite::Statement worker_stmt { db, UpdateWorkerLastActive.data() };
			worker_stmt.bind("@worker_id", worker_id);
			worker_stmt.exec();

			transaction.commit();
		}
	} catch (std::exception & e) {
		std::cout << "[SQLite] Failed to send worker keep alive. SQLite exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}

	std::this_thread::sleep_for(std::chrono::seconds(1));
}

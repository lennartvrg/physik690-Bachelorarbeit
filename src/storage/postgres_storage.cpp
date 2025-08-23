#include <iostream>

#include "storage/postgres_storage.hpp"

#include <mutex>
#include <thread>

#include "schemas/serialize.hpp"

constexpr std::string_view POSTGRES_MIGRATIONS = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	bootstrap_resamples		INTEGER				NOT NULL DEFAULT (100000) CHECK (bootstrap_resamples > 0),
	created_at				BIGINT				NOT NULL,

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
);


CREATE TABLE IF NOT EXISTS "metadata" (
	metadata_id				INTEGER				NOT NULL GENERATED ALWAYS AS IDENTITY,

	simulation_id			INTEGER				NOT NULL,
	algorithm				INTEGER				NOT NULL CHECK (algorithm = 0 OR algorithm = 1),
	num_chunks				INTEGER				NOT NULL DEFAULT (10) CHECK (num_chunks > 0),
	sweeps_per_chunk		INTEGER				NOT NULL DEFAULT (100000) CHECK (sweeps_per_chunk > 0),

	CONSTRAINT "PK.Metadata_SimulationId" PRIMARY KEY (metadata_id),
	CONSTRAINT "FK.Metadata_SimulationId" FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Metadata_SimulationId_Algorithm" ON "metadata" (simulation_id, algorithm);


CREATE TABLE IF NOT EXISTS "workers" (
	worker_id				INTEGER				NOT NULL GENERATED ALWAYS AS IDENTITY,

	name					TEXT				NOT NULL CHECK (length(name) > 0),
	synchronize				BOOLEAN					NULL,
	last_active_at			BIGINT				NOT NULL,


	CONSTRAINT "PK.Workers_WorkerId" PRIMARY KEY (worker_id)
);

CREATE INDEX IF NOT EXISTS "IX.Workers_LastActiveAt" ON "workers" (last_active_at);


CREATE TABLE IF NOT EXISTS "configurations" (
	configuration_id		INTEGER				NOT NULL GENERATED ALWAYS AS IDENTITY,

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
ON CONFLICT (type_id) DO NOTHING;


CREATE TABLE IF NOT EXISTS "estimates" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	worker_id				INTEGER				NOT NULL,
	thread_num				INTEGER				NOT NULL,

	start_time				BIGINT				NOT NULL,
	end_time				BIGINT				NOT NULL CHECK (end_time >= start_time),
	time					BIGINT				GENERATED ALWAYS AS (end_time - start_time) STORED,

	mean					REAL				NOT NULL,
	std_dev					REAL				NOT NULL,

	CONSTRAINT "PK.Estimates_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "FK.Estimates_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Estimates_WorkerId" FOREIGN KEY (worker_id) REFERENCES "workers" (worker_id),
	CONSTRAINT "FK.Estimates_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);


CREATE TABLE IF NOT EXISTS "vortices" (
	vortex_id				INTEGER				NOT NULL GENERATED ALWAYS AS IDENTITY,

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
	spins					BYTEA				NOT NULL,

	CONSTRAINT "PK.VortexResults_VortexId_Sweeps" PRIMARY KEY (vortex_id, sweeps)
);

CREATE OR REPLACE FUNCTION "FNC.RemoveInactiveWorkers"() RETURNS TRIGGER AS
$BODY$
BEGIN
	UPDATE "configurations" SET "active_worker_id" = NULL WHERE "active_worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
	);

	UPDATE "vortices" SET "worker_id" = NULL WHERE "worker_id" IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
	);
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.RemoveInactiveWorkers"
AFTER INSERT ON "workers" FOR EACH ROW
EXECUTE FUNCTION "FNC.RemoveInactiveWorkers"();

CREATE TABLE IF NOT EXISTS "chunks" (
	chunk_id				INTEGER				NOT NULL GENERATED ALWAYS AS IDENTITY,

	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	worker_id				INTEGER				NOT NULL,
	thread_num				INTEGER				NOT NULL,

	start_time				BIGINT				NOT NULL,
	end_time				BIGINT				NOT NULL CHECK (end_time >= start_time),
	time					BIGINT				GENERATED ALWAYS AS (end_time - start_time) STORED,

	spins					BYTEA				NOT NULL,

	CONSTRAINT "PK.Chunks_ChunkId" PRIMARY KEY (chunk_id),
	CONSTRAINT "FK.Chunks_WorkerId" FOREIGN KEY (worker_id) REFERENCES "workers" (worker_id),
	CONSTRAINT "FK.Chunks_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Chunks_ConfigurationId_Index" ON "chunks" ("configuration_id", "index");


CREATE TABLE IF NOT EXISTS "autocorrelations" (
	configuration_id		INTEGER				NOT NULL,
	type_id					INTEGER				NOT NULL,

	chunk_id				INTEGER				NOT NULL,
	data					BYTEA				NOT NULL,

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
	data					BYTEA				NOT NULL,

	CONSTRAINT "PK.Results_ChunkId_TypeId_Index" PRIMARY KEY (chunk_id, type_id),
	CONSTRAINT "FK.Results_ChunkId" FOREIGN KEY (chunk_id) REFERENCES "chunks" (chunk_id),
	CONSTRAINT "FK.Results_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);

CREATE OR REPLACE FUNCTION "FNC.RemoveOutdatedEstimates"() RETURNS TRIGGER AS
$BODY$
BEGIN
	DELETE FROM "estimates" WHERE configuration_id IN (
		SELECT configuration_id FROM "chunks" c WHERE c.chunk_id = NEW.chunk_id
	);
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.RemoveOutdatedEstimates"
AFTER INSERT ON "results" FOR EACH ROW
EXECUTE FUNCTION "FNC.RemoveOutdatedEstimates"();

CREATE OR REPLACE FUNCTION "FNC.OnUpdatedBootstrapResamples"() RETURNS TRIGGER AS
$BODY$
BEGIN
	IF NEW.bootstrap_resamples != OLD.bootstrap_resamples THEN
		DELETE FROM "estimates" WHERE "configuration_id" IN (
			SELECT "configuration_id" FROM "configurations" WHERE "simulation_id" = NEW."simulation_id"
		);
	END IF;
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.OnUpdatedBootstrapResamples"
AFTER UPDATE ON "simulations" FOR EACH ROW
EXECUTE FUNCTION "FNC.OnUpdatedBootstrapResamples"();

CREATE OR REPLACE FUNCTION "FNC.OnInsertedChunk"() RETURNS TRIGGER AS
$BODY$
BEGIN
	UPDATE configurations SET completed_chunks = NEW."index" WHERE configuration_id = NEW.configuration_id;
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.OnInsertedChunk"
BEFORE INSERT ON "chunks" FOR EACH ROW
EXECUTE FUNCTION "FNC.OnInsertedChunk"();
)~~~~~~";

constexpr std::string_view RegisterWorkerQuery = R"~~~~~~(
INSERT INTO "workers" (name, last_active_at) VALUES ($1, CAST(extract(epoch FROM now()) AS int)) RETURNING "worker_id"
)~~~~~~";

PostgresStorage::PostgresStorage(const std::string_view & connection_string) : worker_id(-1), db(connection_string.data()) {
	try {
		pqxx::transaction<pqxx::repeatable_read> transaction { db };
		transaction.exec(POSTGRES_MIGRATIONS.data());

		for (auto [worker_id] : transaction.query<int>(RegisterWorkerQuery.data(), { utils::hostname() })) {
			this->worker_id = worker_id;
		}

		if (this->worker_id == -1) throw std::invalid_argument("Did not insert worker!");
		transaction.commit();
	} catch (const std::exception &e) {
		std::cout << "[Postgres] Failed to migrate database. Postgres exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view InsertSimulationQuery = R"~~~~~~(
INSERT INTO "simulations" (simulation_id, bootstrap_resamples, created_at) VALUES ($1, $2, CAST(extract(epoch FROM now()) AS int))
ON CONFLICT (simulation_id) DO UPDATE SET bootstrap_resamples = $2
)~~~~~~";

constexpr std::string_view InsertVorticesQuery = R"~~~~~~(
INSERT INTO "vortices" (simulation_id, lattice_size) VALUES ($1, $2)
ON CONFLICT (simulation_id, lattice_size) DO NOTHING
)~~~~~~";

constexpr std::string_view InsertMetadataQuery = R"~~~~~~(
INSERT INTO "metadata" (simulation_id, algorithm, num_chunks, sweeps_per_chunk) VALUES ($1, $2, $3, $4)
ON CONFLICT (simulation_id, algorithm) DO UPDATE SET num_chunks = $3 WHERE "metadata".num_chunks < $3
)~~~~~~";

constexpr std::string_view FetchMetadataQuery = R"~~~~~~(
SELECT metadata_id FROM "metadata" WHERE simulation_id = $1 AND algorithm = $2
)~~~~~~";

constexpr std::string_view InsertConfigurationsQuery = R"~~~~~~(
INSERT INTO "configurations" (simulation_id, metadata_id, lattice_size, temperature, depth) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (simulation_id, metadata_id, lattice_size, temperature) DO NOTHING
)~~~~~~";

constexpr std::string_view FetchAllWorkDoneQuery = R"~~~~~~(
SELECT COUNT(*) AS count
FROM configurations c
    INNER JOIN metadata m ON c.metadata_id = m.metadata_id
    LEFT JOIN (
        SELECT c.configuration_id, COUNT(*) AS "done_chunks"
        FROM configurations c INNER JOIN chunks c2 on c.configuration_id = c2.configuration_id
        WHERE c.simulation_id = $1
        GROUP BY c.configuration_id
    ) c2 ON c.configuration_id = c2.configuration_id
    LEFT JOIN (
        SELECT c.configuration_id, COUNT(*) AS "done_estimates"
        FROM configurations c INNER JOIN estimates e ON c.configuration_id = e.configuration_id
        WHERE c.simulation_id = $1
        GROUP BY c.configuration_id
    ) e ON c.configuration_id = e.configuration_id
WHERE c.simulation_id = $1 AND (c2.configuration_id IS NULL OR e.configuration_id IS NULL OR c2.done_chunks < m.num_chunks OR e.done_estimates != CASE WHEN m.algorithm = 0 THEN 8 ELSE 9 END);
)~~~~~~";

constexpr std::string_view FetchMaxDepthQuery = R"~~~~~~(
SELECT MAX(c.depth) AS max_depth
FROM configurations c
WHERE c.simulation_id = $1 AND c.metadata_id = $2 AND c.lattice_size = $3;
)~~~~~~";

constexpr std::string_view FetchPeakMagneticSusceptibilityQuery = R"~~~~~~(
SELECT c.temperature, (c.temperature - LAG(c.temperature, 1) OVER (ORDER BY c.temperature)) AS diff
FROM configurations c
         INNER JOIN estimates e ON c.configuration_id = e.configuration_id AND e.type_id = 5
WHERE c.simulation_id = $1 AND c.metadata_id = $2 AND c.lattice_size = $3
ORDER BY e.mean DESC
LIMIT 1;
)~~~~~~";

bool PostgresStorage::prepare_simulation(const Config config) {
	db.prepare("vortex", InsertVorticesQuery.data());
	db.prepare("insert_metadata", InsertMetadataQuery.data());
	db.prepare("insert_configurations", InsertConfigurationsQuery.data());

	while (true) {
		try {
			pqxx::transaction<pqxx::repeatable_read> transaction { db };

			transaction.exec(InsertSimulationQuery.data(), {
				config.simulation_id,
				config.bootstrap_resamples
			});

			for (const auto size : config.vortex_sizes) {
				transaction.exec(pqxx::prepped { "vortex" }, { config.simulation_id, size });
			}

			for (const auto & [key, value] : config.algorithms) {
				transaction.exec(pqxx::prepped { "insert_metadata" }, {
					config.simulation_id, static_cast<int>(key), value.num_chunks, value.sweeps_per_chunk,
				});

				const auto [metadata_id] = transaction.query1<int>(FetchMetadataQuery.data(), {
					config.simulation_id, static_cast<int>(key)
				});

				for (const auto size : value.sizes) {
					for (const auto temperature : utils::sweep_temperature(0.0, config.max_temperature, config.temperature_steps)) {
						transaction.exec(pqxx::prepped { "insert_configurations" }, {
							config.simulation_id, metadata_id, size, temperature, 1
						});
					}
				}
			}

			// Check if all registered configurations are completely done
			if (get<0>(transaction.query1<int>(FetchAllWorkDoneQuery.data(), { config.simulation_id })) == 0) {
				for (const auto & [key, value] : config.algorithms) {
					// Find metadata row associated with algorithm
					const auto [metadata_id] = transaction.query1<int>(FetchMetadataQuery.data(), {
						config.simulation_id, static_cast<int>(key)
					});

					// Iterate over sizes
					for (const auto size : value.sizes) {
						// Check depth not yet reached
						if (const auto [depth] = transaction.query1<int>(FetchMaxDepthQuery.data(), { config.simulation_id, metadata_id, size }); depth < config.max_depth) {
							// Fetch the Xs peak by the temperature where it occurred and the space to neighboring data points
							const auto pair = transaction.query01<double_t, double_t>(FetchPeakMagneticSusceptibilityQuery.data(), {
								config.simulation_id, metadata_id, static_cast<int>(size)
							});

							if (pair.has_value()) {
								// Extract temperature where xs is max and step size
								const auto [xs_temperature, diff] = *pair;

								// Determine new bounds around the temperature where xs is max
								const auto min_temperature = xs_temperature - 3 * diff;
								const auto max_temperature = xs_temperature + 3 * diff;

								// Add configurations
								for (const auto temperature : utils::sweep_temperature(min_temperature, max_temperature, config.temperature_steps, false)) {
									transaction.exec(pqxx::prepped { "insert_configurations" }, {
										config.simulation_id, metadata_id, size, temperature, depth + 1
									});
								}

							}
						}
					}
				}
			}

			const bool work = get<0>(transaction.query1<int>(FetchAllWorkDoneQuery.data(), { config.simulation_id })) != 0;
			transaction.commit();

			db.unprepare("vortex");
			db.unprepare("insert_metadata");
			db.unprepare("insert_configurations");

			return work;
		} catch (const pqxx::serialization_failure &) {
			std::cout << "[PostgreSQL] Conflict while preparing simulation. Trying again..." << std::endl;
			utils::sleep_between(1000, 3000);
		} catch (std::exception & e) {
			std::cout << "[PostgreSQL] Failed to prepare simulation. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}
	}
}

constexpr std::string_view NextVortexQuery = R"~~~~~~(
WITH selected AS (
	SELECT v."vortex_id", v."lattice_size" FROM "vortices" v WHERE v."simulation_id" = $1 AND NOT EXISTS (
		SELECT * FROM "vortex_results" vr WHERE vr.vortex_id = v.vortex_id
	) AND (
		v."worker_id" IS NULL OR v."worker_id" IN (SELECT w."worker_id" FROM "workers" w WHERE w."last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int))
	) FOR UPDATE OF v
)
UPDATE vortices SET worker_id = $2
FROM selected WHERE worker_id IS NULL AND vortices.vortex_id = selected.vortex_id
RETURNING selected.*;
)~~~~~~";

std::optional<std::tuple<std::size_t, std::size_t>> PostgresStorage::next_vortex(const int simulation_id) {
	while (true) {
		try {
			pqxx::transaction<pqxx::repeatable_read> transaction { db };

			const auto pair = transaction.query01<int, int>(NextVortexQuery.data(), {
				simulation_id, worker_id
			});

			transaction.commit();
			if (!pair.has_value()) {
				return std::nullopt;
			}

			const auto [vortex_id, lattice_size] = *pair;
			return {{ static_cast<std::size_t>(vortex_id), static_cast<std::size_t>(lattice_size) }};
		} catch (const pqxx::serialization_failure &) {
			std::cout << "[PostgreSQL] Conflict while fetching next vortex. Trying again..." << std::endl;
			utils::sleep_between(1000, 3000);
		} catch (std::exception & e) {
			std::cout << "[PostgreSQL] Failed to fetch next vortex. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}
	}
}

constexpr std::string_view InsertVortexResultsQuery = R"~~~~~~(
INSERT INTO "vortex_results" (vortex_id, sweeps, temperature, spins) VALUES ($1, $2, $3, $4)
)~~~~~~";

void PostgresStorage::save_vortices(const std::size_t vortex_id, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>> results) {
	try {
		pqxx::work transaction { db };

		db.prepare("insert_vortex", InsertVortexResultsQuery.data());
		for (const auto & [temperature, sweeps, spins] : results) {
			const auto data = schemas::serialize(spins);

			transaction.exec(pqxx::prepped { "insert_vortex" }, {
				vortex_id, sweeps, temperature, pqxx::binary_cast(data.data(), data.size())
			});
		}

		transaction.commit();
	} catch (std::exception & e) {
		std::cout << "[PostgreSQL] Failed to fetch save vortex results. PostgreSQL exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view NextChunkQuery = R"~~~~~~(
WITH selected AS (
	SELECT c.configuration_id, c.completed_chunks + 1 AS "index", m.algorithm, c.lattice_size, c.temperature, m.sweeps_per_chunk, k.spins
	FROM simulations s
	INNER JOIN configurations c on s.simulation_id = c.simulation_id
	INNER JOIN metadata m ON c.metadata_id = m.metadata_id
	LEFT JOIN chunks k ON c.configuration_id = k.configuration_id AND c.completed_chunks = k."index"
	WHERE s.simulation_id = $1 AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
	)) AND c.completed_chunks < m.num_chunks
	ORDER BY c.completed_chunks ASC, c.lattice_size DESC LIMIT 1
	FOR UPDATE OF s, c, m
)
UPDATE configurations SET active_worker_id = $2
FROM selected WHERE configurations.configuration_id = selected.configuration_id
RETURNING selected.*
)~~~~~~";

std::optional<Chunk> PostgresStorage::next_chunk(const int simulation_id) {
	while (true) {
		try {
			pqxx::transaction<pqxx::repeatable_read> transaction { db };
			const auto configuration_id_opt = transaction.query01<int, int, int, int, double_t, int, std::optional<std::basic_string<std::byte>>>(NextChunkQuery.data(), {
				simulation_id, worker_id
			});
			transaction.commit();

			if (!configuration_id_opt.has_value()) return std::nullopt;
			const auto [configuration_id, index, algorithm, lattice_size, temperature, sweeps_per_chunk, spins_opt] = *configuration_id_opt;

			std::optional<std::vector<double_t>> spins = std::nullopt;
			if (const auto data = spins_opt) {
				spins = schemas::deserialize(data->data());
			}

			return std::optional<Chunk>({
				configuration_id,
				index,
				static_cast<algorithms::Algorithm>(algorithm),
				static_cast<std::size_t>(lattice_size),
				temperature,
				static_cast<std::size_t>(sweeps_per_chunk),
				spins
			});

		} catch (const pqxx::serialization_failure &) {
			std::cout << "[PostgreSQL] Conflict while fetching next chunk. Trying again..." << std::endl;
			utils::sleep_between(1000, 3000);
		} catch (const pqxx::sql_error & e) {
			std::cout << "[PostgreSQL] Failed to fetch next chunk. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}
	}
}

constexpr std::string_view InsertChunkQuery = R"~~~~~~(
INSERT INTO "chunks" (configuration_id, "index", worker_id, thread_num, start_time, end_time, spins) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING chunk_id
)~~~~~~";

constexpr std::string_view InsertAutocorrelationQuery = R"~~~~~~(
INSERT INTO "autocorrelations" (configuration_id, type_id, chunk_id, data) VALUES ($1, $2, $3, $4)
)~~~~~~";

constexpr std::string_view InsertResultQuery = R"~~~~~~(
INSERT INTO "results" (chunk_id, type_id, tau, data) VALUES ($1, $2, $3, $4)
)~~~~~~";

constexpr std::string_view RemoveWorkerQuery = R"~~~~~~(
UPDATE "configurations" SET active_worker_id = NULL WHERE "configuration_id" = $1 AND "active_worker_id" = $2
)~~~~~~";

void PostgresStorage::save_chunk(const Chunk & chunk, const int32_t thread_num, const int64_t start_time, const int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> & results) {
	try {
		pqxx::work transaction { db };

		const auto [chunk_id] = transaction.query1<int>(InsertChunkQuery.data(), {
			chunk.configuration_id, chunk.index, worker_id, thread_num, start_time, end_time, pqxx::binary_cast(spins.data(), spins.size())
		});

		db.prepare("result", InsertResultQuery.data());
		db.prepare("autocorrelation", InsertAutocorrelationQuery.data());

		for (const auto & [key, value] : results) {
			const auto [tau, values, autocorrelation_opt] = value;
			transaction.exec(pqxx::prepped { "result" }, { chunk_id, static_cast<int>(key), tau, pqxx::binary_cast(values.data(), values.size()) });

			if (const auto autocorrelation = autocorrelation_opt) {
				transaction.exec(pqxx::prepped { "autocorrelation" }, {
					chunk.configuration_id, static_cast<int>(key), chunk_id, pqxx::binary_cast(autocorrelation->data(), autocorrelation->size())
				});
			}
		}

		transaction.exec(RemoveWorkerQuery.data(), { chunk.configuration_id, worker_id });
		transaction.commit();

		db.unprepare("result");
		db.unprepare("autocorrelation");
	} catch (std::exception & e) {
		std::cout << "[PostgreSQL] Failed to save chunk. PostgreSQL exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view FetchNextEstimateQuery = R"~~~~~~(
WITH selected AS (
	SELECT c.configuration_id, s.bootstrap_resamples, m.num_chunks, t.type_id
	FROM "simulations" s
	INNER JOIN "configurations" c ON c.simulation_id = s.simulation_id
	INNER JOIN "metadata" m ON c.metadata_id = m.metadata_id
	INNER JOIN "chunks" ch ON c.configuration_id = ch.configuration_id AND ch."index" = m.num_chunks
	CROSS JOIN "types" t
	LEFT JOIN "estimates" e ON e.configuration_id = c.configuration_id AND e.type_id = t.type_id
	WHERE s.simulation_id = $1 AND c.completed_chunks = m.num_chunks AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
	)) AND e.type_id IS NULL AND (t.type_id BETWEEN 0 AND 3 OR t.type_id = 6 OR (t.type_id = 8 AND m.algorithm = 1))
	LIMIT 1 FOR UPDATE OF s, c, m, ch
)
UPDATE configurations SET active_worker_id = $2
FROM selected WHERE configurations.configuration_id = selected.configuration_id
RETURNING selected.*
)~~~~~~";

constexpr std::string_view FetchConfigurationResults = R"~~~~~~(
SELECT c."index", r.data
FROM "chunks" c
INNER JOIN "results" r ON c.chunk_id = r.chunk_id AND r.type_id = $2
WHERE c.configuration_id = $1
ORDER BY c."index"
)~~~~~~";

std::optional<std::tuple<Estimate, std::vector<double_t>>> PostgresStorage::next_estimate(const int simulation_id) {
	while (true) {
		try {
			pqxx::transaction<pqxx::repeatable_read> transaction { db };

			const auto estimate_opt = transaction.query01<int, std::size_t, int, int>(FetchNextEstimateQuery.data(), {
				simulation_id, worker_id
			});

			if (!estimate_opt.has_value()) {
				return std::nullopt;
			}

			const auto [configuration_id, bootstrap_resamples, num_chunks, type] = *estimate_opt;
			transaction.commit();

			pqxx::work work { db };

			std::vector<double_t> values {};
			for (const auto & [index, buffer] : work.query<int, std::basic_string<std::byte>>(FetchConfigurationResults.data(), { configuration_id, type })) {
				const auto data = schemas::deserialize(buffer.data());

				values.reserve(data.size() * num_chunks);
				values.insert(values.end(), data.begin(), data.end());
			}

			work.commit();

			return { std::make_tuple<Estimate, std::vector<double_t>>({ configuration_id, static_cast<observables::Type>(type), bootstrap_resamples }, std::move(values)) };
		} catch (const pqxx::serialization_failure &) {
			std::cout << "[PostgreSQL] Conflict while fetching next estimate. Trying again..." << std::endl;
			utils::sleep_between(1000, 3000);
		}  catch (std::exception & e) {
			std::cout << "[PostgreSQL] Failed to fetch next estimate. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}
	}
}

constexpr std::string_view InsertEstimateQuery = R"~~~~~~(
INSERT INTO "estimates" (configuration_id, type_id, worker_id, thread_num, start_time, end_time, mean, std_dev) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
)~~~~~~";

void PostgresStorage::save_estimate(const int configuration_id, const int32_t thread_num, const int64_t start_time, const int64_t end_time, const observables::Type type, const double_t mean, const double_t std_dev) {
	try {
		pqxx::work transaction { db };

		transaction.exec(InsertEstimateQuery.data(), {
			configuration_id, static_cast<int>(type), worker_id, thread_num, start_time, end_time, mean, std_dev
		});

		transaction.exec(RemoveWorkerQuery.data(), {
			configuration_id, worker_id,
		});

		transaction.commit();
	} catch (const std::exception & e) {
		std::cout << "[PostgreSQL] Failed to save estimate. PostgreSQL exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view FetchNextDerivativeQuery = R"~~~~~~(
WITH selected AS (
	SELECT e.configuration_id, e.type_id, c.temperature, e.mean, e.std_dev, o.mean, o.std_dev
	FROM "estimates" e
	INNER JOIN "configurations" c ON e.configuration_id = c.configuration_id AND c.simulation_id = $1
	INNER JOIN "estimates" o ON e.configuration_id = o.configuration_id AND o.type_id = CASE WHEN e.type_id = 0 THEN 1 WHEN e.type_id = 2 THEN 3 ELSE 0 END
	LEFT JOIN "estimates" t ON e.configuration_id = t.configuration_id AND t.type_id = CASE WHEN e.type_id = 0 THEN 4 WHEN e.type_id = 2 THEN 5 ELSE 7 END
	WHERE (e.type_id = 0 OR e.type_id = 2 OR e.type_id = 6) AND (c.active_worker_id IS NULL OR c.active_worker_id IN (
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
	)) AND (t.configuration_id IS NULL)
	LIMIT 1 FOR UPDATE OF e, c, o
)
UPDATE configurations SET active_worker_id = $2
FROM selected WHERE configurations.configuration_id = selected.configuration_id
RETURNING selected.*
)~~~~~~";

std::optional<NextDerivative> PostgresStorage::next_derivative(const int simulation_id) {
	while (true) {
		try {
			pqxx::transaction<pqxx::repeatable_read> transaction { db };

			const auto derivative_opt = transaction.query01<int, int, double_t, double_t, double_t, double_t, double_t>(FetchNextDerivativeQuery.data(), {
				simulation_id, worker_id
			});

			transaction.commit();
			if (!derivative_opt.has_value()) {
				return std::nullopt;
			}

			const auto [ configuration_id, type, temperature, mean, std_dev, square_mean, square_std_dev ] = derivative_opt.value();
			return { { configuration_id, static_cast<observables::Type>(type), temperature, mean, std_dev, square_mean, square_std_dev } };
		}  catch (const pqxx::serialization_failure &) {
			std::cout << "[PostgreSQL] Conflict while fetching next derivative. Trying again..." << std::endl;
			utils::sleep_between(1000, 3000);
		}  catch (const std::exception & e) {
			std::cout << "[PostgreSQL] Failed to fetch next derivative. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}
	}
}

constexpr std::string_view UpdateWorkerLastActive = R"~~~~~~(
UPDATE "workers" SET "last_active_at" = CAST(extract(epoch FROM now()) AS int) WHERE "worker_id" = $1;
)~~~~~~";

void PostgresStorage::worker_keep_alive() {
	try {
		pqxx::work transaction { db };
		transaction.exec(UpdateWorkerLastActive.data(), pqxx::params { worker_id });
		transaction.commit();
	} catch (const std::exception & e) {
		std::cout << "[PostgreSQL] Failed to send worker keep alive. PostgreSQL exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

constexpr std::string_view SetWorkerSynchronizeFlag = R"~~~~~~(
UPDATE "workers" SET "synchronize" = $2 WHERE "worker_id" = $1
)~~~~~~";

constexpr std::string_view FetchWorkerSynchronizeFlag = R"~~~~~~(
SELECT "synchronize" IS NULL FROM "workers" WHERE "worker_id" = $1
)~~~~~~";

constexpr std::string_view CountActiveWorkerNotWaitingForSynchronization = R"~~~~~~(
SELECT COUNT(*) FROM "workers" WHERE "last_active_at" >= CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int) AND "synchronize" IS NULL
)~~~~~~";

constexpr std::string_view ResetWaitingFlag = R"~~~~~~(
UPDATE "workers" SET "synchronize" = NULL WHERE "last_active_at" >= CAST(extract(epoch FROM now() - INTERVAL '5 minutes') AS int)
)~~~~~~";

void PostgresStorage::synchronize_workers() {
	try {
		pqxx::work transaction { db };
		transaction.exec(SetWorkerSynchronizeFlag.data(), pqxx::params { worker_id, true });
		transaction.commit();
	} catch (const std::exception & e) {
		std::cout << "[PostgreSQL] Failed to send worker keep alive. PostgreSQL exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}

	while (true) {
		try {
			pqxx::work transaction { db };

			const auto [exit] = transaction.query1<bool>(FetchWorkerSynchronizeFlag.data(), pqxx::params { worker_id });
			if (exit) {
				break;
			}

			const auto [not_waiting] = transaction.query1<int>(CountActiveWorkerNotWaitingForSynchronization.data());
			if (not_waiting == 0) {
				transaction.exec(ResetWaitingFlag.data());
			}

			transaction.exec(UpdateWorkerLastActive.data(), pqxx::params { worker_id });
			transaction.commit();
		} catch (const std::exception & e) {
			std::cout << "[PostgreSQL] Failed to send worker keep alive. PostgreSQL exception: " << e.what() << std::endl;
			std::rethrow_exception(std::current_exception());
		}

		// Wait a bit
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
}

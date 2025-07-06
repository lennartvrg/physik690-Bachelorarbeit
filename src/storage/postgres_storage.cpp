#include <iostream>

#include "storage/postgres_storage.hpp"

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
		SELECT "worker_id" FROM "workers" WHERE "last_active_at" < extract(epoch FROM now() - INTERVAL '-5 minutes')
	);

	DELETE FROM "workers" WHERE NOT EXISTS (
		SELECT * FROM "configurations" c WHERE c."active_worker_id" = "worker_id"
	) AND NOT EXISTS (
		SELECT * FROM "chunks" c WHERE c."worker_id" = "worker_id"
	) AND "last_active_at" < extract(epoch FROM now() - INTERVAL '-5 minutes');
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.RemoveInactiveWorkers"
AFTER INSERT ON "workers" FOR EACH ROW
EXECUTE FUNCTION "FNC.RemoveInactiveWorkers"();

CREATE TABLE IF NOT EXISTS "chunks" (
	chunk_id				INTEGER				NOT NULL,

	configuration_id		INTEGER				NOT NULL,
	"index"					INTEGER				NOT NULL,

	worker_id				INTEGER				NOT NULL,

	start_time				INTEGER				NOT NULL,
	end_time				INTEGER				NOT NULL CHECK (end_time >= start_time),
	time					INTEGER				GENERATED ALWAYS AS (end_time - start_time) STORED,

	spins					BYTEA				NOT NULL,

	CONSTRAINT "PK.Chunks_ChunkId" PRIMARY KEY (chunk_id),
	CONSTRAINT "FK.Chunks_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Chunks_WorkerId" FOREIGN KEY (worker_id) REFERENCES "workers" (worker_id)
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
	DELETE FROM "estimates" WHERE type_id = NEW.type_id AND EXISTS (
		SELECT * FROM "chunks" c WHERE c.chunk_id = NEW.chunk_id AND configuration_id = c.configuration_id
	);
	RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER "TRG.RemoveOutdatedEstimates"
AFTER INSERT ON "results" FOR EACH ROW
EXECUTE FUNCTION "FNC.RemoveOutdatedEstimates"();
)~~~~~~";

constexpr std::string_view RegisterWorkerQuery = R"~~~~~~(
INSERT INTO "workers" (name, last_active_at) VALUES ($1, $2) RETURNING "worker_id"
)~~~~~~";

PostgresStorage::PostgresStorage(const std::string_view & connection_string) : worker_id(-1), db(connection_string.data()) {
	try {
		pqxx::work transaction { db };
		transaction.exec(POSTGRES_MIGRATIONS.data());

		for (auto [worker_id] : transaction.query<int>(RegisterWorkerQuery.data(), { utils::hostname(), utils::timestamp_ms() })) {
			this->worker_id = worker_id;
		}

		if (this->worker_id == -1) throw std::invalid_argument("Did not insert worker!");
		transaction.commit();
	} catch (const std::exception &e) {
		std::cout << "[Postgres] Failed to migrate database. Postgres exception: " << e.what() << std::endl;
		std::rethrow_exception(std::current_exception());
	}
}

bool PostgresStorage::prepare_simulation(Config config) {
	return true;
}

std::optional<std::tuple<std::size_t, std::size_t>> PostgresStorage::next_vortex(int simulation_id) {
	return std::nullopt;
}

void PostgresStorage::save_vortices(std::size_t vortex_id, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>>) {
}

std::optional<Chunk> PostgresStorage::next_chunk(int simulation_id) {
	return std::nullopt;
}

void PostgresStorage::save_chunk(const Chunk &chunk, int64_t start_time, int64_t end_time, const std::span<const uint8_t> &spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> & results) {
}

std::optional<std::tuple<Estimate, std::vector<double_t>>> PostgresStorage::next_estimate(int simulation_id) {
	return std::nullopt;
}

std::optional<NextDerivative> PostgresStorage::next_derivative(int simulation_id) {
	return std::nullopt;

}

void PostgresStorage::save_estimate(int configuration_id, int64_t start_time, int64_t end_time, observables::Type type, double_t mean, double_t std_dev) {
}

void PostgresStorage::worker_keep_alive() {
}

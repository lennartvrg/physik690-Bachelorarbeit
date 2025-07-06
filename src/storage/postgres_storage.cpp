#include <iostream>

#include "storage/postgres_storage.hpp"
#include "storage/migrations.hpp"

constexpr std::string_view POSTGRES_MIGRATIONS = R"~~~~~~(
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

PostgresStorage::PostgresStorage(const std::string_view & connection_string) : worker_id(-1), db(connection_string.data()) {
	try {
		pqxx::work transaction { db };
		transaction.exec(MIGRATIONS.data());
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

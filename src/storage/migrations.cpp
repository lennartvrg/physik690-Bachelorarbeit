#include <string_view>

constexpr std::string_view MIGRATIONS = R"~~~~~~(
CREATE TABLE IF NOT EXISTS "simulations" (
	simulation_id			INTEGER				NOT NULL,

	bootstrap_resamples		INTEGER				NOT NULL DEFAULT (100000) CHECK (bootstrap_resamples > 0),
	created_at				BIGINT				NOT NULL,

	CONSTRAINT "PK.Simulations_SimulationId" PRIMARY KEY (simulation_id)
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

	temperature_denominator	INTEGER				NOT NULL CHECK (temperature_denominator > 0),
	temperature_numerator	INTEGER				NOT NULL CHECK (temperature_numerator > 0),
	temperature				REAL				NOT NULL GENERATED ALWAYS AS (CAST(temperature_numerator AS REAL) / CAST(temperature_denominator AS REAL)),

	CONSTRAINT "PK.Configurations_ConfigurationId" PRIMARY KEY (configuration_id),
	CONSTRAINT "FK.Configurations_ActiveWorkerId" FOREIGN KEY (active_worker_id) REFERENCES "workers" (worker_id),
	CONSTRAINT "FK.Configurations_SimulationId"	FOREIGN KEY (simulation_id) REFERENCES "simulations" (simulation_id),
	CONSTRAINT "FK.Configurations_MetadataId" FOREIGN KEY (metadata_id) REFERENCES "metadata" (metadata_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS "IX.Configurations_MetadataId_Algorithm_LatticeSize_Temperature" ON "configurations" (simulation_id, metadata_id, lattice_size, temperature_denominator, temperature_numerator);

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
	time					INTEGER				GENERATED ALWAYS AS (end_time - start_time),

	mean					REAL				NOT NULL,
	std_dev					REAL				NOT NULL,

	CONSTRAINT "PK.Estimates_ConfigurationId_TypeId" PRIMARY KEY (configuration_id, type_id),
	CONSTRAINT "FK.Estimates_ConfigurationId" FOREIGN KEY (configuration_id) REFERENCES "configurations" (configuration_id),
	CONSTRAINT "FK.Estimates_TypeId" FOREIGN KEY (type_id) REFERENCES "types" (type_id)
);
)~~~~~~";
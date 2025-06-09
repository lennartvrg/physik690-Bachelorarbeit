#ifndef SQLITE_STORAGE_HPP
#define SQLITE_STORAGE_HPP

#include <SQLiteCpp/Database.h>

#include "storage.hpp"

class SQLiteStorage final : public Storage {
public:
	explicit SQLiteStorage(const std::string_view & path);

	void prepare_simulation(Config config) override;

	std::optional<Chunk> next_chunk(int simulation_id) override;

	void save_chunk(const Chunk & chunk, int64_t start_time, int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>>> & results) override;

	std::optional<std::tuple<Estimate, std::vector<double_t>>> next_estimate(int simulation_id) override;

	std::optional<NextDerivative> next_derivative(int simulation_id) override;

	void save_estimate(int configuration_id, int64_t start_time, int64_t end_time, observables::Type type, double_t mean, double_t std_dev) override;

	void worker_keep_alive() override;

private:
	int worker_id;
	SQLite::Database db;
};

#endif //SQLITE_STORAGE_HPP

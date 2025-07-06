#ifndef POSTGRES_STORAGE_HPP
#define POSTGRES_STORAGE_HPP

#include <pqxx/pqxx>

#include "storage.hpp"

class PostgresStorage final : public Storage {
public:
	explicit PostgresStorage(const std::string_view & connection_string);

	bool prepare_simulation(Config config) override;

	std::optional<std::tuple<std::size_t, std::size_t>> next_vortex(int simulation_id) override;

	void save_vortices(std::size_t vortex_id, std::vector<std::tuple<double_t, std::size_t, std::vector<double_t>>>) override;

	std::optional<Chunk> next_chunk(int simulation_id) override;

	void save_chunk(const Chunk & chunk, int64_t start_time, int64_t end_time, const std::span<const uint8_t> & spins, const std::map<observables::Type, std::tuple<double_t, std::vector<uint8_t>, std::optional<std::vector<uint8_t>>>> & results) override;

	std::optional<std::tuple<Estimate, std::vector<double_t>>> next_estimate(int simulation_id) override;

	std::optional<NextDerivative> next_derivative(int simulation_id) override;

	void save_estimate(int configuration_id, int64_t start_time, int64_t end_time, observables::Type type, double_t mean, double_t std_dev) override;

	void worker_keep_alive() override;

private:
	int worker_id{};
	pqxx::connection db;
};

#endif //POSTGRES_STORAGE_HPP

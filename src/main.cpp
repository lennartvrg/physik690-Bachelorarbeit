#include <iostream>
#include <sqlite3.h>

#include "config.hpp"
#include "lattice.hpp"
#include "algorithms/metropolis.hpp"
#include "algorithms/wolff.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"
#include "storage/sqlite_storage.hpp"
#include "utils/task_distributor.hpp"

using SimulationResult = std::tuple<double, double, double, double, double, double>;

static algorithms::function sweeps[2] = { algorithms::metropolis, algorithms::wolff };

SimulationResult simulate_size(const Chunk & chunk) {
    Lattice lattice {chunk.lattice_size, 1.0 / chunk.temperature, chunk.spins};
    std::mt19937 rng {std::random_device{}()};

    auto [energy, magnets] = algorithms::simulate(lattice, chunk.sweeps, rng, sweeps[chunk.algorithm]);

    const auto [e_tau, _1] = analysis::integrated_autocorrelation_time(energy);
    const auto [e_mean, e_stddev] = analysis::bootstrap(rng, energy, e_tau, 100);

    const auto [m_tau, _2] = analysis::integrated_autocorrelation_time(magnets);
    const auto [m_mean, m_stddev] = analysis::bootstrap(rng, magnets, m_tau, 100);

    return {e_tau, e_mean, e_stddev, m_tau, m_mean, m_stddev};
}

std::optional<Chunk> fetch_next(const std::shared_ptr<SQLiteStorage> & storage, const std::size_t simulation_id) {
    return storage->next_chunk(static_cast<int>(simulation_id));
}

static std::atomic_size_t counter;

void save_chunk(const Chunk & chunk, const SimulationResult & result, [[maybe_unused]] const std::shared_ptr<SQLiteStorage> & storage, [[maybe_unused]] const std::size_t simulation_id) {
    std::cout << "ConfigurationId: " << chunk.configuration_id << " has mean energy: " << get<1>(result) << std::endl;
    ++counter;
}

int main() {
    try {
        const auto config = Config::from_file("config.toml");

        const auto storage = std::make_shared<SQLiteStorage>();
        storage->prepare_simulation(config);

        const auto next = std::function<std::optional<Chunk>(std::shared_ptr<SQLiteStorage>, std::size_t)>(fetch_next);
        const auto task = std::function<SimulationResult(Chunk)>(simulate_size);
        const auto save = std::function<void(Chunk, SimulationResult, std::shared_ptr<SQLiteStorage>, std::size_t)>(save_chunk);
        utils::distribute_tasks(next, task, save, storage, config.simulation_id);

        std::cout << "Counter: " << counter << std::endl;

        return 0;
    } catch (const std::exception & e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

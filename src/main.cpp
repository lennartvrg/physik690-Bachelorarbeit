#include <algorithm>
#include <iostream>

#include "config.hpp"
#include "algorithms/metropolis.hpp"
#include "storage/sqlite_storage.hpp"

#include "tasks/simulation.hpp"
#include "tasks/bootstrap.hpp"
#include "tasks/derivatives.hpp"

static XoshiroCpp::Xoshiro256Plus rng { std::random_device {}() };

std::vector<std::tuple<utils::ratio, std::size_t, std::vector<double_t>>> simulate_vortices(const std::size_t size) {
    std::size_t sweeps = 100000;
    Lattice lattice { size, utils::ratio { 1, 2 }, std::nullopt };

    // Thermalize
    std::cout << "[Vortices] Thermalizing for " << sweeps << " sweeps" << std::endl;
    algorithms::simulate(lattice, rng, sweeps, algorithms::METROPOLIS);

    // Transition from hot to cold state
    std::vector<std::tuple<utils::ratio, std::size_t, std::vector<double_t>>> results;
    for (const auto temperature : utils::sweep_temperature_rev(2.0, 90)) {
        std::cout << "[Vortices] Simulating at t " << std::fixed << std::setprecision(2) << temperature.approx() << std::endl;
        lattice.set_beta(temperature.inverse().approx());

        // Stay at temperature
        for (const std::size_t _ : std::views::iota(0, 20)) {
            algorithms::simulate(lattice, rng, 1, algorithms::METROPOLIS);
            results.emplace_back(temperature, ++sweeps, lattice.get_spins());
        }
    }

    // Wait for vortices to dissolve
    for (const auto _ : std::views::iota(0, 2000)) {
        algorithms::simulate(lattice, rng, 20, algorithms::METROPOLIS);
        results.emplace_back(get<0>(results.at(results.size() - 1)), sweeps += 20, lattice.get_spins());
    }

    return results;
}

int main() {
    try {
        // Read configuration from TOML
        const auto config = Config::from_file("config.toml");

        // Prepare the database for simulation
        auto storage = std::make_shared<SQLiteStorage>("output/data.db");
        storage->prepare_simulation(config);

        // Simulate vortices
        while (const auto pair = storage->next_vortex(config.simulation_id)) {
            const auto [vortex_id, lattice_size] = *pair;
            const auto results = simulate_vortices(lattice_size);
            storage->save_vortices(vortex_id, results);
        }

        // Run simulation, bootstrap the results and calculate the derivatives
        tasks::Simulation<SQLiteStorage> { config, storage }.execute();
        tasks::Bootstrap<SQLiteStorage> { config, storage }.execute();
        tasks::Derivatives<SQLiteStorage> { config, storage }.execute();

        return 0;
    } catch (const std::exception & e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

#include <iostream>

#include "config.hpp"
#include "storage/sqlite_storage.hpp"

#include "tasks/simulation.hpp"
#include "tasks/bootstrap.hpp"
#include "tasks/derivatives.hpp"
#include "tasks/vortices.hpp"

int main() {
    try {
        // Read configuration from TOML
        const auto config = Config::from_file("config.toml");
        auto storage = std::make_shared<SQLiteStorage>("output/data.db");

        // Prepare the database for simulation
        while (storage->prepare_simulation((config))) {
            tasks::Vortices<SQLiteStorage> { config, storage }.execute();
            tasks::Simulation<SQLiteStorage> { config, storage }.execute();
            tasks::Bootstrap<SQLiteStorage> { config, storage }.execute();
            tasks::Derivatives<SQLiteStorage> { config, storage }.execute();
        }

        return 0;
    } catch (const std::exception & e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

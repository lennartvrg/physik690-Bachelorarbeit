#include <iostream>

#include "config.hpp"
#include "storage/postgres_storage.hpp"
#include "storage/sqlite_storage.hpp"

#include "tasks/simulation.hpp"
#include "tasks/bootstrap.hpp"
#include "tasks/derivatives.hpp"
#include "tasks/vortices.hpp"

template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
int run(std::string_view connection_string) {
    // Read configuration from TOML
    const auto config = Config::from_file("config.toml");
    auto storage = std::make_shared<TStorage>(connection_string);

    // Prepare the database for simulation
    while (storage->prepare_simulation((config))) {
        tasks::Vortices<TStorage> { config, storage }.execute();
        tasks::Simulation<TStorage> { config, storage }.execute();
        tasks::Bootstrap<TStorage> { config, storage }.execute();
        tasks::Derivatives<TStorage> { config, storage }.execute();
    }

    return 0;
}

int main() {
    try {
        //return run<SQLiteStorage>("output/data.db");
        return run<PostgresStorage>("postgres://postgres:postgres@10.4.0.129:5432/Bachelor");
    } catch (const std::exception & e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

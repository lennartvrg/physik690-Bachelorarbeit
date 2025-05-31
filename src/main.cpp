#include <iostream>

#include "config.hpp"
#include "storage/sqlite_storage.hpp"
#include "tasks/simulation.hpp"


int main() {
    try {
        const auto config = Config::from_file("config.toml");

        tasks::Simulation<SQLiteStorage> { config, "output/data.db" }.execute();

        return 0;
    } catch (const std::exception & e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

#include <storage/sqlite.hpp>

std::tuple<algorithms::Algorithm, std::size_t> SQLite2::next_configuration() {
	

	return { algorithms::Algorithm::METROPOLIS, 20 };
}

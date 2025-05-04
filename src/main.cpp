#include <iostream>
#include <random>

#include "lattice.hpp"
#include "metropolis.hpp"

static thread_local std::mt19937 thread_rng {std::random_device{}()};

void simulate_size(const std::size_t size, std::mt19937 & rng, const algorithms::function & algorithm) {
    Lattice lattice {size, 1.0 / 3.0};
    auto [energy, magnetization] = algorithm(lattice, 100000, rng);

    double avg_energy = std::reduce(energy.begin(), energy.end()) / static_cast<double>(energy.size());
    double avg_magnet = std::reduce(magnetization.begin(), magnetization.end()) / static_cast<double>(magnetization.size());

    std::cout << "Average energy: " << avg_energy << std::endl;
    std::cout << "Average magnetization: " << avg_magnet << std::endl;
}

int main() {
    simulate_size(32, thread_rng, algorithms::metropolis);
}

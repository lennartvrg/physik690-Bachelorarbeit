#include <iostream>
#include <chrono>

#include "lattice.hpp"

int main() {
    auto lattice = Lattice(2 * 4096, 1.0);

    auto t1 = std::chrono::high_resolution_clock::now();
    auto energy = lattice.energy();
    auto t2 = std::chrono::high_resolution_clock::now();

    const auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    std::cout << energy / static_cast<double>(lattice.num_sites()) << " in " << dur << std::endl;
}
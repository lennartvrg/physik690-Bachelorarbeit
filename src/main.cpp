#include <iomanip>
#include <iostream>
#include <random>
#include <ranges>
#include <execution>
#include <fstream>

#include "lattice.hpp"
#include "wolff.hpp"
#include "metropolis.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"

static std::mutex mtx;

static thread_local std::mt19937 thread_rng {std::random_device{}()};

static std::vector<double> sweep_through_temperature() {
    std::vector<double> result (150);
    std::ranges::generate(result, [n = 0.0] mutable{ return n += 0.02; });
    return result;
}

template<typename T>
void write_output_csv(const std::span<const T> measurements, const std::string & file_name, const std::string & headers) {
    std::ofstream output;
    output.open("output/" + file_name + ".csv");

    output << headers << "\n";
    std::ranges::copy(measurements, std::ostream_iterator<T>(output, "\n"));
    output.close();
}

std::tuple<double, double, double> simulate_size(const std::size_t size, const double temperature, std::mt19937 & rng, const algorithms::function & algorithm) {
    Lattice lattice {size, 1.0 / temperature};
    auto [energy, _1] = algorithms::simulate(lattice, 40000, rng, algorithm);

    const auto [tau, _2] = analysis::integrated_autocorrelation_time(energy);
    const auto [mean, stddev] = analysis::bootstrap(rng, energy, tau, 100);

    return {tau, mean, stddev};
}

int main() {
    const auto range = sweep_through_temperature();
    std::vector<std::string> results {};
    results.resize(range.size());

    std::atomic_uint64_t counter {0};
    std::transform(std::execution::par_unseq, range.begin(), range.end(), results.begin(), [&] (const double temperature) {
        const auto [tau, energy, magnetization] = simulate_size(32, temperature, thread_rng, algorithms::wolff);

        std::lock_guard lock {mtx};
        std::cout << "\r\t L=64 " << ++counter << "/" << 150 << " | T = " << temperature << std::flush;

        std::ostringstream os;
        os << temperature << "," << tau << "," << energy << "," << magnetization;

        return os.str();
    });

    const std::span<const std::string> span = results;
    write_output_csv(span, "wolff", "temperature,tau,energy,energy_stddev");
}

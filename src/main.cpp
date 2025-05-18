#include <iomanip>
#include <iostream>
#include <random>
#include <ranges>
#include <execution>
#include <filesystem>
#include <fstream>

#include "lattice.hpp"
#include "wolff.hpp"
#include "metropolis.hpp"
#include "analysis/autocorrelation.hpp"
#include "analysis/boostrap.hpp"

static std::mutex mtx;

static std::vector<double> sweep_through_temperature() {
    std::vector<double> result (64);
    std::ranges::generate(result, [&, n = 0.0] mutable {
        return n += 3.0 / static_cast<double>(result.size());
    });
    return result;
}

void write_output_csv(const std::ostringstream & measurements, const std::string & file_name, const std::string & headers) {
    std::filesystem::create_directories("output");

    std::ofstream output;
    output.open("output/" + file_name + ".csv");

    output << headers << std::endl << measurements.str();
    output.close();
}

std::tuple<double, double, double, double, double, double> simulate_size(const std::size_t size, const double temperature, const algorithms::function & algorithm) {
    Lattice lattice {size, 1.0 / temperature};
    std::mt19937 rng {std::random_device{}()};

    auto [energy, magnets] = algorithms::simulate(lattice, 40000, rng, algorithm);

    const auto [e_tau, _1] = analysis::integrated_autocorrelation_time(energy);
    const auto [e_mean, e_stddev] = analysis::bootstrap(rng, energy, e_tau, 100);

    const auto [m_tau, _2] = analysis::integrated_autocorrelation_time(magnets);
    const auto [m_mean, m_stddev] = analysis::bootstrap(rng, magnets, m_tau, 100);

    return {e_tau, e_mean, e_stddev, m_tau, m_mean, m_stddev};
}

int main() {
    std::ostringstream os;
    const auto range = sweep_through_temperature();

    std::atomic_uint64_t counter {0};
    std::for_each(std::execution::par, range.begin(), range.end(), [&] (const double temperature) {
        const auto [e_tau, e_mean, e_stddev, m_tau, m_mean, m_stddev] = simulate_size(512, temperature, algorithms::wolff);

        std::scoped_lock lock {mtx};
        std::cout << "\r\t L=64 " << ++counter << "/64 | T = " << temperature << std::flush;
        os << temperature << "," << e_tau << "," << e_mean << "," << e_stddev << "," << m_tau << "," << m_mean << "," << m_stddev << std::endl;
    });

    write_output_csv(os, "wolff", "temperature,e_tau,e_mean,e_stddev,m_tau,m_mean,m_stddev");
}

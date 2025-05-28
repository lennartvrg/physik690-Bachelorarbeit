#include <cassert>

#include <simde/x86/svml.h>
#include <simde/x86/avx.h>
#include <simde/x86/sse2.h>

#include "lattice.hpp"
#include "utils.hpp"
#include "algorithms/algorithms.hpp"

Lattice::Lattice(const std::size_t length, const double beta) : beta(beta), length(length), spins(length * length) {
    assert(length * length % 4 == 0 && "Lattice size must be a multiple of 4 as the SIMD instructions won't work otherwise");
}

double Lattice::get(const std::size_t i) const noexcept {
    return spins[i % num_sites()];
}

void Lattice::set(const std::size_t i, const double angle) noexcept {
    assert(angle >= 0.0 && angle < algorithms::N_PI<2> && "Angle must be on [0.0, 2PI)");
    spins[i % num_sites()] = angle;
}

double Lattice::energy() const noexcept {
    simde__m256d result = simde_mm256_setzero_pd();
    for (std::size_t i = 0; i < num_sites(); i += 2) {
        const simde__m256d old = simde_mm256_set_pd(get(i), get(i), get(i + 1), get(i + 1));
        const simde__m256d neighbours = simde_mm256_set_pd(get(i + 1), get(i + length), get(i + 2),
                                                     get(i + 1 + length));

        const simde__m256d diff = simde_mm256_sub_pd(neighbours, old);
        const simde__m256d cos = simde_mm256_cos_pd(diff);

        result = simde_mm256_add_pd(cos, result);
    }
    return -mm256_reduce_add_pd(result);
}

double Lattice::energy_diff(const std::size_t i, const double angle) const noexcept {
    const simde__m256d neighbours = simde_mm256_set_pd(get(i + 1), get(i + num_sites() - 1), get(i + length),
                                                 get(i + num_sites() - length));

    const simde__m256d a = simde_mm256_set1_pd(get(i));
    const simde__m256d before = simde_mm256_cos_pd(simde_mm256_sub_pd(a, neighbours));

    const simde__m256d b = simde_mm256_set1_pd(angle);
    const simde__m256d after = simde_mm256_cos_pd(simde_mm256_sub_pd(b, neighbours));

    return mm256_reduce_add_pd(before) - mm256_reduce_add_pd(after);
}

std::tuple<double, double> Lattice::magnetization() const noexcept {
    simde__m256d result = simde_mm256_setzero_pd();
    for (std::size_t i = 0; i < num_sites(); i += 4) {
        const simde__m256d data = simde_mm256_set_pd(spins[i], spins[i + 1], spins[i + 2], spins[i + 3]);

        simde__m256d cos = simde_mm256_setzero_pd();
        const simde__m256d sin = simde_mm256_sincos_pd(&cos, data);

        const simde__m256d add = simde_mm256_hadd_pd(cos, sin);
        result = simde_mm256_add_pd(add, result);
    }
    return {result[0] + result[2], result[1] + result[3]};
}

std::tuple<double, double> Lattice::magnetization_diff(const std::size_t i, const double angle) const noexcept {
    const simde__m128d data = simde_mm_set_pd(angle, algorithms::N_PI<1> + get(i));

    simde__m128d cos = simde_mm_setzero_pd();
    const simde__m128d sin = simde_mm_sincos_pd(&cos, data);

    return {cos[0] + cos[1], sin[0] + sin[1]};
}

double Lattice::acceptance(const double energy_diff) const noexcept {
    return std::min(1.0, std::exp(-beta * energy_diff));
}

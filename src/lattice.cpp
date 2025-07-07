#include <cassert>

#include <simde/x86/svml.h>
#include <simde/x86/avx2.h>
#include <simde/x86/sse2.h>

#include "lattice.hpp"
#include "utils/utils.hpp"
#include "algorithms/algorithms.hpp"

Lattice::Lattice(const std::size_t length, const double_t beta, const std::optional<std::vector<double_t>> & spins) : beta(beta), length(length),
        spins(spins.has_value() ? utils::aligned_vector<double_t> { spins.value().begin(), spins.value().end() } : utils::aligned_vector<double_t>(length * length)) {
    assert(length * length % 4 == 0 && "Lattice size must be a multiple of 4 as the SIMD instructions won't work otherwise");
    assert(beta > 0.0 && "Beta must be greater than zero");
    assert(this->spins.size() == length * length && "Number of spins must be L^2");
}

void Lattice::set(const std::size_t i, const double_t angle) noexcept {
    assert(angle >= 0.0 && angle < algorithms::N_PI<2> && "Angle must be on [0.0, 2PI)");
    spins[i % num_sites()] = angle;
}

double_t Lattice::energy() const noexcept {
    simde__m256d result = simde_mm256_setzero_pd();
    for (std::size_t i = 0; i < num_sites(); i += 2) {
        const simde__m256d old = simde_mm256_set_pd(spins[i], spins[i], spins[i + 1], spins[i + 1]);
        const simde__m256d neighbours = simde_mm256_set_pd(spins[shift_col(i, 1)], spins[shift_row(i, 1)],
            spins[shift_col(i + 1, 1)], spins[shift_row(i + 1, 1)]);

        const simde__m256d diff = simde_mm256_sub_pd(old, neighbours);
        const simde__m256d cos = simde_mm256_cos_pd(diff);

        result = simde_mm256_add_pd(result, cos);
    }
    return -utils::mm256_reduce_add_pd(result);
}

double_t Lattice::energy_diff(const std::size_t i, const double_t angle) const noexcept {
    const simde__m256d neighbours = simde_mm256_set_pd(spins[shift_col(i, 1)], spins[shift_col(i, -1)], spins[shift_row(i, 1)], spins[shift_row(i, -1)]);

    const simde__m256d a = simde_mm256_set1_pd(spins[i]);
    const simde__m256d before = simde_mm256_cos_pd(simde_mm256_sub_pd(a, neighbours));

    const simde__m256d b = simde_mm256_set1_pd(angle);
    const simde__m256d after = simde_mm256_cos_pd(simde_mm256_sub_pd(b, neighbours));

    return utils::mm256_reduce_add_pd(before) - utils::mm256_reduce_add_pd(after);
}

double_t Lattice::helicity_modulus() const noexcept {
    simde__m256d result = simde_mm256_setzero_pd();
    for (std::size_t i = 0; i < num_sites(); i += 4) {
        const simde__m256d data = simde_mm256_load_pd(spins.data() + i);
        const simde__m256d neighbours = simde_mm256_set_pd(spins[shift_col(i + 3, 1)], spins[shift_col(i + 2, 1)], spins[shift_col(i + 1, 1)], spins[shift_col(i, 1)]);

        const simde__m256d pi = simde_mm256_set1_pd(algorithms::N_PI<2>);
        const simde__m256d diff = simde_mm256_sub_pd(simde_mm256_add_pd(data, pi), neighbours);

        const simde__m256d sin = simde_mm256_sin_pd(diff);
        result = simde_mm256_add_pd(result, sin);
    }
    return utils::mm256_reduce_add_pd(result);
}

double_t Lattice::helicity_modulus_diff(const std::size_t i, const double_t angle) const noexcept {
    const simde__m256d data = simde_mm256_set_pd(spins[i], angle, spins[shift_col(i, -1)], spins[shift_col(i, -1)]);
    const simde__m256d neighbours = simde_mm256_set_pd(spins[shift_col(i, 1)], spins[shift_col(i, 1)], spins[i], angle);

    const simde__m256d pi = simde_mm256_set1_pd(algorithms::N_PI<2>);
    const simde__m256d diff = simde_mm256_sub_pd(simde_mm256_add_pd(data, pi), neighbours);

    const simde__m256d sin = simde_mm256_sin_pd(diff);
    return sin[0] + sin[2] - (sin[1] + sin[3]);
}

std::tuple<double_t, double_t> Lattice::magnetization() const noexcept {
    simde__m256d result = simde_mm256_setzero_pd();
    for (std::size_t i = 0; i < num_sites(); i += 4) {
        const simde__m256d data = simde_mm256_load_pd(spins.data() + i);

        simde__m256d cos = simde_mm256_setzero_pd();
        const simde__m256d sin = simde_mm256_sincos_pd(&cos, data);

        const simde__m256d add = simde_mm256_hadd_pd(cos, sin);
        result = simde_mm256_add_pd(result, add);
    }
    return {result[0] + result[2], result[1] + result[3]};
}

std::tuple<double_t, double_t> Lattice::magnetization_diff(const std::size_t i, const double_t angle) const noexcept {
    const simde__m128d data = simde_mm_set_pd(angle, algorithms::N_PI<1> + spins[i]);

    simde__m128d cos = simde_mm_setzero_pd();
    const simde__m128d sin = simde_mm_sincos_pd(&cos, data);

    return {cos[0] + cos[1], sin[0] + sin[1]};
}

double_t Lattice::acceptance(const double_t energy_diff) const noexcept {
    return std::min(1.0, std::exp(-beta * energy_diff));
}

std::vector<double_t> Lattice::get_spins() const noexcept {
    return { this->spins.begin(), this->spins.end() };
}
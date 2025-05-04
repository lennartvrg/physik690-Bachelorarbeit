#include <cassert>
#include <cmath>

#include <simde/x86/svml.h>
#include <simde/x86/avx2.h>
#include <simde/x86/avx.h>
#include <simde/x86/sse4.2.h>

#include "lattice.hpp"

Lattice::Lattice(const std::size_t length, const double beta) : beta(beta), length(length), spins(length * length) {
    assert(length * length % 4 == 0);
}

constexpr std::size_t Lattice::side_length() const noexcept {
    return length;
}

std::size_t Lattice::num_sites() const noexcept {
    return length * length;
}

double Lattice::angle(const std::size_t i) const noexcept {
    return spins[i % num_sites()];
}

void Lattice::set_angle(const std::size_t i, const double angle) noexcept {
    assert(angle >= 0.0 && angle < 2.0 * std::numbers::pi && "Angle must be on [0.0, 2PI)");
    spins[i % num_sites()] = angle;
}

double mm256_reduce_add_pd(simde__m256d v) {
    simde__m128d low = simde_mm256_castpd256_pd128(v);
    simde__m128d high = simde_mm256_extractf128_pd(v, 1);
    low = simde_mm_add_pd(low, high);

    simde__m128d high64 = simde_mm_unpackhi_pd(low, low);
    return simde_mm_cvtsd_f64(simde_mm_add_sd(low, high64));
}

double Lattice::energy() const noexcept {
    double result = 0.0;
    for (std::size_t i = 0; i < num_sites(); i += 2) {
        simde__m256d old = simde_mm256_set_pd(angle(i), angle(i), angle(i + 1), angle(i + 1));
        simde__m256d neighbours = simde_mm256_set_pd(angle(i + 1), angle(i + length), angle(i + 2), angle(i + 1 + length));

        simde__m256d diff = simde_mm256_sub_pd(neighbours, old);
        simde__m256d cos = simde_mm256_cos_pd(diff);

        result += mm256_reduce_add_pd(cos);
    }
    return -result;
}

double Lattice::energy_diff(const std::size_t i, const double angle) const noexcept {

    return 0.0;
}

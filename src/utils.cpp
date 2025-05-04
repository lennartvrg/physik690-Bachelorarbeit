#include <simde/x86/sse2.h>

#include "utils.hpp"

double mm256_reduce_add_pd(simde__m256d v) {
    simde__m128d low = simde_mm256_castpd256_pd128(v);
    simde__m128d high = simde_mm256_extractf128_pd(v, 1);
    low = simde_mm_add_pd(low, high);

    simde__m128d high64 = simde_mm_unpackhi_pd(low, low);
    return simde_mm_cvtsd_f64(simde_mm_add_sd(low, high64));
}

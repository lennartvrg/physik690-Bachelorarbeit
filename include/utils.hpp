#ifndef UTILS_HPP
#define UTILS_HPP

#include <string>
#include <vector>
#include <simde/x86/avx.h>

std::string hostname();

double mm256_reduce_add_pd(simde__m256d v);

std::vector<double> sweep_through_temperature(double max_temperature, std::size_t steps);

#endif //UTILS_HPP

#include "analysis/autocorrelation.hpp"

#include <algorithm>
#include <cassert>
#include <complex>
#include <numeric>
#include <fftw3.h>
#include <mutex>
#include <ranges>

static std::mutex mtx;

static std::complex<double_t> fold(const std::complex<double_t> x) {
	return x * std::conj(x);
}

static void discrete_fourier_transform(const std::span<std::complex<double_t>> & in, const std::span<std::complex<double_t>> & out, const int direction) {
	assert(in.size() == out.size() && "Input and output vectors must be of same size");
	std::unique_lock lock {mtx};

	const auto plan = fftw_plan_dft_1d(static_cast<int>(in.size()), reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]), direction, FFTW_ESTIMATE);
	lock.unlock();

	fftw_execute_dft(plan, reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]));
	fftw_destroy_plan(plan);
}

std::vector<double_t> normalized_autocorrelation_function(const std::span<double_t> & data) {
	assert(!data.empty() && "Input data must not be empty");
	const auto mean = std::ranges::fold_left(data, 0.0, std::plus()) / static_cast<double_t>(data.size());

	std::vector<std::complex<double_t>> in (data.size()), out (data.size());
	std::ranges::transform(data, in.begin(), [&] (const auto x) {
		return std::complex<double_t>(x - mean, 0.0);
	});

	discrete_fourier_transform(in, out, FFTW_FORWARD);
	std::ranges::transform(out, out.begin(), fold);

	discrete_fourier_transform(out, in, FFTW_BACKWARD);
	const auto norm = 1.0 / in[0].real();

	std::vector<double_t> result (in.size());
	std::ranges::transform(in, result.begin(), [&] (const auto c) {
		return c.real() * norm;
	});

	return result;
}

std::tuple<double_t, std::vector<double_t>> analysis::integrated_autocorrelation_time(const std::span<double_t> & data) {
	const auto correlation = normalized_autocorrelation_function(data);
	const auto positive = correlation | std::ranges::views::drop(1) | std::ranges::views::take_while([] (const double_t x) {
		return x > 0.0;
	});

	const auto tau = std::ranges::fold_left(positive, 0.5, std::plus());
	return {tau, correlation};
}

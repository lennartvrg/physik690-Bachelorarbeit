#include "analysis/autocorrelation.hpp"

#include <algorithm>
#include <cassert>
#include <complex>
#include <numeric>
#include <fftw3.h>
#include <mutex>
#include <ranges>

static std::mutex mtx;

static std::complex<double> fold(const std::complex<double> x) {
	return x * std::conj(x);
}

static void discrete_fourier_transform(std::span<std::complex<double>> in, std::span<std::complex<double>> out, const int direction) {
	std::unique_lock lock {mtx};
	const auto p = fftw_plan_dft_1d(static_cast<int>(in.size()), reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]), direction, FFTW_ESTIMATE);
	lock.unlock();

	fftw_execute_dft(p, reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]));
	fftw_destroy_plan(p);
}

std::vector<double> normalized_autocorrelation_function(const std::span<double> & data) {
	assert(!data.empty() && "Input data must not be empty");
	const auto mean = std::ranges::fold_left(data, 0.0, std::plus()) / static_cast<double>(data.size());

	std::vector<std::complex<double>> in (data.size()), out (data.size());
	std::ranges::transform(data, in.begin(), [&] (const auto x) {
		return std::complex<double>(x - mean, 0.0);
	});

	discrete_fourier_transform(in, out, FFTW_FORWARD);
	std::ranges::transform(out, out.begin(), fold);
	discrete_fourier_transform(out, in, FFTW_BACKWARD);

	std::vector<double> result (data.size());
	std::ranges::transform(in, result.begin(), [&] (const auto c) {
		return c.real() / in[0].real();
	});

	return result;
}

std::tuple<double, std::vector<double>> analysis::integrated_autocorrelation_time(const std::span<double> & data) {
	const auto correlation = normalized_autocorrelation_function(data);
	const auto positive = correlation | std::ranges::views::drop(1) | std::ranges::views::take_while([] (const double x) {
		return x > 0.0;
	});

	const auto tau = 0.5 + std::ranges::fold_left(positive, 0.0, [] (const double sum, const double c) { return sum + c; });
	return {tau, correlation};
}

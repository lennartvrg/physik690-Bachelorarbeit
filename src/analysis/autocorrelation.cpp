#include "analysis/autocorrelation.hpp"

#include <algorithm>
#include <assert.h>
#include <complex>
#include <numeric>
#include <fftw3.h>
#include <mutex>
#include <ranges>

static std::mutex mtx;

std::vector<double> normalized_autocorrelation_function(const std::span<double> & data) {
	assert(data.size() > 0 && "Input data must not be empty");
	const auto mean = std::accumulate(data.begin(), data.end(), 0.0) / static_cast<double>(data.size());

	std::vector<std::complex<double>> in = {}, out = {};
	for (const double x : data) {
		in.push_back(std::complex {x - mean, 0.0});
		out.push_back({ 0.0, 0.0 });
	}

	std::unique_lock lock {mtx};
	const auto p = fftw_plan_dft_1d(in.size(), reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]), FFTW_FORWARD, FFTW_ESTIMATE);
	lock.unlock();

	fftw_execute_dft(p, reinterpret_cast<fftw_complex*>(&in[0]), reinterpret_cast<fftw_complex*>(&out[0]));
	fftw_destroy_plan(p);

	for (std::size_t i = 0; i < out.size(); ++i) {
		out[i] = out[i] * std::conj(out[i]);
	}

	lock.lock();
	const auto q = fftw_plan_dft_1d(in.size(), reinterpret_cast<fftw_complex*>(&out[0]), reinterpret_cast<fftw_complex*>(&in[0]), FFTW_BACKWARD, FFTW_ESTIMATE);
	lock.unlock();

	fftw_execute_dft(q, reinterpret_cast<fftw_complex*>(&out[0]), reinterpret_cast<fftw_complex*>(&in[0]));
	fftw_destroy_plan(q);

	std::vector<double> result {};
	for (const std::complex c : in) {
		result.push_back(c.real() / in[0].real());
	}

	return result;
}

std::tuple<double, std::vector<double>> analysis::integrated_autocorrelation_time(const std::span<double> & data) {
	const auto correlation = normalized_autocorrelation_function(data);
	const auto positive = correlation | std::ranges::views::drop(1) | std::ranges::views::take_while([] (const double x) { return x > 0.0; });

	const auto tau = 0.5 + std::ranges::fold_left(positive, 0.0, [](double sum, const double c) { return sum += c; });
	return {tau, correlation};
}
#ifndef DERIVATIVES_HPP
#define DERIVATIVES_HPP

#include "task.hpp"
#include "storage/next_derivative.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Derivatives final : public Task<TStorage, NextDerivative, std::tuple<observables::Type, double_t, double_t>> {
	public:
		template<typename ... Args>
		explicit Derivatives(const Config & config, Args && ... args) : Task<TStorage, NextDerivative, std::tuple<observables::Type, double_t, double_t>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<NextDerivative> next_task() override {
			return this->storage->next_derivative(this->config.simulation_id);
		}

		std::tuple<observables::Type, double_t, double_t> execute_task(const NextDerivative & task) override {
			if (task.type == observables::Energy || task.type == observables::EnergySquared) {
				const auto [mean, std_dev] = specific_heat(task.temperature, task.mean, task.std_dev, task.square_mean, task.square_std_dev);
				return { observables::Type::SpecificHeat, mean, std_dev };
			}

			if (task.type == observables::Magnetization || task.type == observables::MagnetizationSquared) {
				const auto [mean, std_dev] = magnetic_susceptibility(task.temperature, task.mean, task.std_dev, task.square_mean, task.square_std_dev);
				return { observables::Type::MagneticSusceptibility, mean, std_dev };
			}

			throw std::invalid_argument("Derivative type is neither energy or magnetization");
		}

		void save_task(const NextDerivative & task, const std::tuple<observables::Type, double_t, double_t> & result) override {
			std::cout << "ConfigurationId: " << task.configuration_id << " | Type: " << get<0>(result) << std::endl;
			this->storage->save_estimate(task.configuration_id, get<0>(result), get<1>(result), get<2>(result));
		}

	private:
		static std::tuple<double_t, double_t> specific_heat(const double_t temperature, const double_t mean, const double_t std_dev, const double_t square_mean, const double_t square_std_dev) {
			const auto cs_mean = (square_mean - std::pow(mean, 2)) / std::pow(temperature, 2);
			const auto cv_std_dev = std::sqrt(std::pow(square_std_dev / std::pow(temperature, 2), 2) + std::pow(2.0 * mean * std_dev / std::pow(temperature, 2), 2));
			return { cs_mean, cv_std_dev };
		}

		static std::tuple<double_t, double_t> magnetic_susceptibility(const double_t temperature, const double_t mean, const double_t std_dev, const double_t square_mean, const double_t square_std_dev) {
			const auto xs_mean = (square_mean - std::pow(mean, 2)) / temperature;
			const auto xs_std_dev = std::sqrt(std::pow(square_std_dev / temperature, 2) + std::pow(2.0 * mean * std_dev / temperature, 2));
			return { xs_mean, xs_std_dev };
		}
	};
}

#endif //DERIVATIVES_HPP

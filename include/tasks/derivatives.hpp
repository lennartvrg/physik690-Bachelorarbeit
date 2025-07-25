#ifndef DERIVATIVES_HPP
#define DERIVATIVES_HPP

#include <cmath>

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
		std::optional<NextDerivative> next_task(std::shared_ptr<TStorage> storage) override {
			return storage->next_derivative(this->config.simulation_id);
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

			if (task.type == observables::HelicityModulusIntermediate) {
				const auto [mean, std_dev] = helicity_modulus(task.temperature, task.mean, task.std_dev, task.square_mean, task.square_std_dev);
				return { observables::Type::HelicityModulus, mean, std_dev };
			}

			throw std::invalid_argument("Derivative type is neither energy or magnetization");
		}

		void save_task(std::shared_ptr<TStorage> storage, const NextDerivative & task, int32_t thread_num, int64_t start_time, int64_t end_time, const std::tuple<observables::Type, double_t, double_t> & result) override {
			std::cout << "[Derivatives] ConfigurationId: " << task.configuration_id << " | Type: " << get<0>(result) << std::endl;
			storage->save_estimate(task.configuration_id, thread_num, start_time, end_time, get<0>(result), get<1>(result), get<2>(result));
		}

	private:
		static std::tuple<double_t, double_t> specific_heat(const double_t temperature, const double_t mean, const double_t std_dev, const double_t square_mean, const double_t square_std_dev) {
			const auto norm = 1.0 / std::pow(temperature, 2.0);
			const auto cv_mean = norm * (square_mean - std::pow(mean, 2.0));
			const auto cv_std_dev = std::sqrt(std::pow(square_std_dev * norm, 2.0) + std::pow(2.0 * mean * std_dev * norm, 2.0));
			return { cv_mean, cv_std_dev };
		}

		static std::tuple<double_t, double_t> magnetic_susceptibility(const double_t temperature, const double_t mean, const double_t std_dev, const double_t square_mean, const double_t square_std_dev) {
			const auto norm = 1.0 / temperature;
			const auto xs_mean = norm * (square_mean - std::pow(mean, 2.0));
			const auto xs_std_dev = std::sqrt(std::pow(square_std_dev * norm, 2.0) + std::pow(2.0 * mean * std_dev * norm, 2.0));
			return { xs_mean, xs_std_dev };
		}

		static std::tuple<double_t, double_t> helicity_modulus(const double_t temperature, const double_t helicity, const double_t helicity_std_dev, const double_t energy_mean, const double_t energy_std_dev) {
			const auto norm = 1.0 / temperature;
			const auto hm_mean = -energy_mean / 2.0 - norm * helicity;
			const auto hm_std_dev = std::sqrt(std::pow(-energy_std_dev / 2.0, 2.0) + std::pow(norm * helicity_std_dev, 2.0));
			return { hm_mean, hm_std_dev };
		}
	};
}

#endif //DERIVATIVES_HPP

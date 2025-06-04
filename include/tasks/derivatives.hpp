#ifndef DERIVATIVES_HPP
#define DERIVATIVES_HPP

#include "task.hpp"
#include "storage/next_derivative.hpp"

namespace tasks {
	template<typename TStorage> requires std::is_base_of_v<Storage, TStorage>
	class Derivatives final : public Task<TStorage, NextDerivative, std::tuple<observables::Type, double>> {
	public:
		template<typename ... Args>
		explicit Derivatives(const Config & config, Args && ... args) : Task<TStorage, NextDerivative, std::tuple<observables::Type, double>>(config, std::forward<Args>(args)...) {

		}

	protected:
		std::optional<NextDerivative> next_task() override {
			return this->storage->next_derivative(this->config.simulation_id);
		}

		std::tuple<observables::Type, double> execute_task(const NextDerivative & task) override {
			if (task.type == observables::Energy || task.type == observables::EnergySquared) {
				return { observables::Type::SpecificHeat, (task.square_mean - std::pow(task.mean, 2)) / std::pow(task.temperature, 2) };
			}

			if (task.type == observables::Magnetization || task.type == observables::MagnetizationSquared) {
				return { observables::Type::MagneticSusceptibility, (task.square_mean - std::pow(task.mean, 2)) / task.temperature };
			}

			throw std::invalid_argument("Derivative type is neither energy or magnetization");
		}

		void save_task(const NextDerivative & task, const std::tuple<observables::Type, double> & result) override {
			std::cout << "ConfigurationId: " << task.configuration_id << " | Type: " << get<0>(result) << std::endl;
			this->storage->save_estimate(task.configuration_id, get<0>(result), get<1>(result), 0.0);
		}
	};
}

#endif //DERIVATIVES_HPP

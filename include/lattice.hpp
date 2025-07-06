#ifndef LATTICE_HPP
#define LATTICE_HPP

#include <optional>
#include <vector>

#include "utils/utils.hpp"

class Lattice {
public:
    Lattice(std::size_t length, double_t beta, const std::optional<std::vector<double_t>> & spins);

    [[nodiscard]] constexpr std::size_t side_length() const noexcept {
        return length;
    }

    [[nodiscard]] constexpr std::size_t num_sites() const noexcept {
        return length * length;
    }

    [[nodiscard]] double_t get_beta() const noexcept {
        return beta;
    }

    void set_beta(const double_t beta) noexcept {
        this->beta = beta;
    }

    [[nodiscard]] constexpr std::size_t shift_row(std::size_t i, int32_t delta) const noexcept {
        const auto sites = num_sites();
        return (i + sites + delta * length) % sites;
    }

    [[nodiscard]] constexpr std::size_t shift_col(std::size_t i, int32_t delta) const noexcept {
        const auto sites = num_sites();
        const auto row = i % sites / length * length;
        const auto col = (i % length + length + delta) % length;
        return row + col;
    }

    void set(std::size_t i, double_t angle) noexcept;
    double operator[] (const std::size_t i) const { return spins[i]; }

    [[nodiscard]] double_t energy() const noexcept;
    [[nodiscard]] double_t energy_diff(std::size_t i, double_t angle) const noexcept;

    [[nodiscard]] double_t helicity_modulus() const noexcept;
    [[nodiscard]] double_t helicity_modulus_diff(std::size_t i, double_t angle) const noexcept;

    [[nodiscard]] std::tuple<double_t, double_t> magnetization() const noexcept;
    [[nodiscard]] std::tuple<double_t, double_t> magnetization_diff(std::size_t i, double_t angle) const noexcept;

    [[nodiscard]] double_t acceptance(double_t energy_diff) const noexcept;

    [[nodiscard]] std::vector<double_t> get_spins() const noexcept;

private:
    double_t beta;
    const std::size_t length;
    utils::aligned_vector<double_t> spins;
};

#endif
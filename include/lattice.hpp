#ifndef LATTICE_HPP
#define LATTICE_HPP

#include <optional>
#include <vector>

class Lattice {
public:
    Lattice(std::size_t length, double_t beta, const std::optional<std::vector<double_t>> & spins);

    [[nodiscard]] constexpr std::size_t side_length() const noexcept {
        return length;
    }

    [[nodiscard]] constexpr std::size_t num_sites() const noexcept {
        return length * length;
    }

    [[nodiscard]] constexpr double_t get_beta() const noexcept {
        return beta;
    }

    void set(std::size_t i, double_t angle) noexcept;
    double operator[] (std::size_t i) const { return spins[i]; }

    [[nodiscard]] double_t energy() const noexcept;
    [[nodiscard]] double_t energy_diff(std::size_t i, double_t angle) const noexcept;

    [[nodiscard]] std::tuple<double_t, double_t> magnetization() const noexcept;
    [[nodiscard]] std::tuple<double_t, double_t> magnetization_diff(std::size_t i, double_t angle) const noexcept;

    [[nodiscard]] double_t acceptance(double_t energy_diff) const noexcept;

    [[nodiscard]] std::vector<double_t> get_spins() const noexcept;

private:
    double_t beta;
    std::size_t length;
    std::vector<double_t> spins;
};

#endif
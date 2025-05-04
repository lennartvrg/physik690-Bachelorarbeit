#ifndef LATTICE_HPP
#define LATTICE_HPP

#include <vector>

class Lattice {
public:
    Lattice(std::size_t length, double beta);

    [[nodiscard]] constexpr std::size_t side_length() const noexcept {
        return length;
    }

    [[nodiscard]] constexpr std::size_t num_sites() const noexcept {
        return length * length;
    };


    [[nodiscard]] double get(std::size_t i) const noexcept;
    void set(std::size_t i, double angle) noexcept;

    [[nodiscard]] double energy() const noexcept;
    [[nodiscard]] double energy_diff(std::size_t i, double angle) const noexcept;

    [[nodiscard]] std::tuple<double, double> magnetization() const noexcept;
    [[nodiscard]] std::tuple<double, double> magnetization_diff(std::size_t i, double angle) const noexcept;

    double acceptance(double energy_diff) const noexcept;

private:
    double beta;
    std::size_t length;
    std::vector<double> spins;
};

#endif
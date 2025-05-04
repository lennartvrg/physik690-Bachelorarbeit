#ifndef LATTICE_HPP
#define LATTICE_HPP

#include <vector>

class Lattice {
public:
    Lattice(std::size_t length, double beta);

    [[nodiscard]] constexpr std::size_t side_length() const noexcept;
    [[nodiscard]] std::size_t num_sites() const noexcept;


    [[nodiscard]] double angle(std::size_t i) const noexcept;
    void set_angle(std::size_t i, double angle) noexcept;

    double energy() const noexcept;
    double energy_diff(std::size_t i, double angle) const noexcept;

    double magnetization() const noexcept;
    double magnetization_diff(std::size_t i, double angle) const noexcept;

    double acceptance(double energy_diff) const noexcept;

private:
    double beta;
    std::size_t length;
    std::vector<double> spins;
};

#endif
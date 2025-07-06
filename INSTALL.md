# Ubuntu packages
- meson
- flatbuffers-compiler
- g++-14
- libpqxx-dev
- libsimde-dev
- libboost-all-dev
- libfftw3-dev
- libtomlplusplus-dev
- libflatbuffers-dev
- libsqlite3-dev
- libtbb-dev

# Set GCC 14 as default
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100
sudo update-alternatives --config g++

# Build
- git clone https://github.com/lennartvrg/physik690-Bachelorarbeit
- meson setup --buildtype=release buildDir/

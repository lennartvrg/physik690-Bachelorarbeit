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
- CC=icx CXX=icpx meson setup --reconfigure --buildtype=release buildDir/ .

# Intel Compiler
- wget https://registrationcenter-download.intel.com/akdlm/IRC_NAS/bd1d0273-a931-4f7e-ab76-6a2a67d646c7/intel-oneapi-base-toolkit-2025.2.0.592_offline.sh
- chmod +x intel-oneapi-base-toolkit-2025.2.0.592_offline.sh
- ./intel-oneapi-base-toolkit-2025.2.0.592_offline.sh -a --silent --eula accep
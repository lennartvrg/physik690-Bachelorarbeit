project(
  'xy_model',
  'cpp',
  version : '0.1',
  meson_version : '>= 1.3.0',
  default_options : ['warning_level=2', 'cpp_std=c++23'],
  subproject_dir: 'thirdparty'
)

sqlite = subproject('SQLiteCpp')
sqlite_dep = sqlite.get_variable('sqlitecpp_dep')

simde = subproject('simde')
simde_dep = simde.get_variable('simde_dep')

tomlplusplus = subproject('tomlplusplus')
tomlplusplus_dep = tomlplusplus.get_variable('tomlplusplus_dep')

boost_dep = dependency('boost', required: true)
fftw3_dep = dependency('fftw3', version: '>= 3.0.0', required: true)

dependencies = [
  sqlite_dep,
  simde_dep,
  fftw3_dep,
  boost_dep,
  tomlplusplus_dep
]

incdir = include_directories(['include', 'thirdparty/Xoshiro-cpp'])
run_command('flatc', '--cpp', '-o', 'include/schemas', 'schemas/vector.fbs', check: true)

exe = executable(
  'xy_model',
  'src/utils/utils.cpp',
  'src/utils/ratio.cpp',
  'src/schemas/serialize.cpp',
  'src/config.cpp',
  'src/lattice.cpp',
  'src/algorithms/algorithms.cpp',
  'src/algorithms/metropolis.cpp',
  'src/algorithms/wolff.cpp',
  'src/observables/type.cpp',
  'src/analysis/autocorrelation.cpp',
  'src/analysis/bootstrap.cpp',
  'src/storage/migrations.cpp',
  'src/storage/sqlite_storage.cpp',
  'src/main.cpp',
  cpp_args: ['-march=native', '-ffast-math'],
  link_args: ['-march=native', '-ltbb'],
  include_directories: incdir,
  install : true,
  dependencies : dependencies,
)

project(
  'xy_model',
  'cpp',
  version : '0.1',
  meson_version : '>= 1.8.0',
  default_options : ['warning_level=2', 'cpp_std=c++26'],
  subproject_dir: 'thirdparty'
)

sqlite = subproject('SQLiteCpp')
sqlite_dep = sqlite.get_variable('sqlitecpp_dep')

simde = subproject('simde')
simde_dep = simde.get_variable('simde_dep')

dependencies = [
  sqlite_dep,
  simde_dep
]

incdir = include_directories('include')

exe = executable(
  'xy_model',
  'src/utils.cpp',
  'src/lattice.cpp',
  'src/algorithms.cpp',
  'src/metropolis.cpp',
  'src/wolff.cpp',
  'src/main.cpp',
  cpp_args: ['-march=native', '-fno-math-errno', '-fno-trapping-math'],
  link_args: '-ltbb',
  include_directories: incdir,
  install : true,
  dependencies : dependencies,
)

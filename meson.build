project('chamelio-benchmarks', ['c'],
  version : '1.0',
  default_options: [
    'c_std=gnu99',
    'optimization=3',
    'debug=true',
    'c_args=-march=native -Wno-address-of-packed-member',
  ]
)

# TODO: Maybe we need to link dpdk statically for better performance
cc = meson.get_compiler('c')
threads_dep = dependency('threads', required: true)
dpdk_dep = dependency('libdpdk', required: true)

demi_inc = include_directories('/root/include')
demi_lib = cc.find_library(
  'demikernel',
  dirs: ['/root/lib'],
  required: true,
)
demi_dep = declare_dependency(
  include_directories: demi_inc,
  dependencies: [
    demi_lib,
    dpdk_dep,
  ],
)

subdir('udp')
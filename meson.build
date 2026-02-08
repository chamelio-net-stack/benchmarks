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
dpdk_dep = dependency('libdpdk', required: false)

demi_lib = cc.find_library(
  'demikernel',
  dirs: ['/workspaces/lib'],
  required: false,
)

cham_dep = dependency('cham_udp', required: false)

demi_enabled = dpdk_dep.found() and demi_lib.found()
cham_enabled = dpdk_dep.found() and cham_dep.found()

if demi_enabled
  demi_inc = include_directories('/workspaces/include')
  demi_dep = declare_dependency(
    include_directories: demi_inc,
    dependencies: [
      demi_lib,
      dpdk_dep,
    ],
    link_args: [
      '-Wl,-rpath,/workspaces/lib',
      '-Wl,-rpath,/workspaces/lib/x86_64-linux-gnu',
    ],
  )
endif

if cham_enabled
  cham_deps = declare_dependency(
    dependencies: [
      cham_dep,
      dpdk_dep,
    ],
  )
endif

subdir('udp')
subdir('kvs')

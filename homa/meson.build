if cham_homa_dep.found()
  homa_common = files('homa_cham.c', '../utils/core.c')
  homa_cham_server = executable(
    'homa_cham_server',
    'homa_cham_server.c',
    homa_common,
    dependencies: [cham_homa_dep, threads_dep],
  )
  homa_cham_client = executable(
    'homa_cham_client',
    'homa_cham_client.c',
    homa_common,
    '../utils/clock.c',
    '../utils/hist.c',
    dependencies: [cham_homa_dep, threads_dep],
  )
  alias_target('homa_cham', [homa_cham_client, homa_cham_server])
endif

fs = import('fs')
homa_module_dir = run_command('sh', '-c',
  'printf %s "${HOMA_MODULE_DIR:-/opt/HomaModule}"', check: true).stdout()
if fs.is_file(homa_module_dir / 'homa.h')
  homa_linux_common = files('homa_linux.c', '../utils/core.c')
  homa_linux_server = executable(
    'homa_linux_server',
    'homa_linux_server.c',
    homa_linux_common,
    include_directories: include_directories(homa_module_dir),
    dependencies: threads_dep,
  )
  homa_linux_client = executable(
    'homa_linux_client',
    'homa_linux_client.c',
    homa_linux_common,
    '../utils/clock.c',
    '../utils/hist.c',
    include_directories: include_directories(homa_module_dir),
    dependencies: threads_dep,
  )
  alias_target('homa_linux', [homa_linux_client, homa_linux_server])
endif

# eTran exposes its Homa RPC API in C++; keep the benchmark workloads in C.
etran_dir = run_command('sh', '-c',
  'printf %s "${ETRAN_DIR:-/opt/eTran}"', check: true).stdout()
if fs.is_file(etran_dir / 'eTran/lib/include/eTran_rpc.h') and fs.is_file(
    etran_dir / 'eTran/shared_lib/libetran.so')
  add_languages('cpp', required: true)
  cpp = meson.get_compiler('cpp')
  etran_lib = cpp.find_library('etran',
    dirs: [etran_dir / 'eTran/shared_lib'])
  etran_inc = include_directories(
    etran_dir / 'eTran/lib/include',
    etran_dir / 'eTran/common',
    etran_dir / 'lib/install/include',
  )
  etran_deps = [etran_lib, threads_dep,
    cpp.find_library('xdp', dirs: [etran_dir / 'lib/install/lib']),
    cpp.find_library('bpf', dirs: [etran_dir / 'lib/install/lib']),
    cpp.find_library('elf'), cpp.find_library('z'), cpp.find_library('dl')]
  homa_etran_common = files('homa_etran.c', 'homa_etran_transport.cc',
    '../utils/core.c')
  homa_etran_client = executable(
    'homa_etran_client', 'homa_etran_client.c', homa_etran_common,
    '../utils/clock.c', '../utils/hist.c',
    include_directories: etran_inc,
    dependencies: etran_deps,
    override_options: ['cpp_std=gnu++17'],
  )
  homa_etran_server = executable(
    'homa_etran_server', 'homa_etran_server.c', homa_etran_common,
    include_directories: etran_inc,
    dependencies: etran_deps,
    override_options: ['cpp_std=gnu++17'],
  )
  alias_target('homa_etran', [homa_etran_client, homa_etran_server])
endif

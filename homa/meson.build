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

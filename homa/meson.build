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
    dependencies: [cham_homa_dep, threads_dep],
  )
  alias_target('homa_cham', [homa_cham_client, homa_cham_server])
endif

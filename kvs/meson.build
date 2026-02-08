kvs_inc = include_directories('common/include')

kvs_server_common = files(
  'server/hashtable.c',
  'server/ialloc.c',
  'server/jenkins_hash.c',
  'server/settings.c',
)

kvs_bench_common = files(
  'bench/rng.c',
  'bench/commandline.c',
  'bench/workload.c',
)

libm = cc.find_library('m', required: true)
librt = cc.find_library('rt', required: false)

kvs_deps = [
  threads_dep,
  libm,
]

if librt.found()
  kvs_deps += librt
endif

flexkvs = executable(
  'flexkvs',
  kvs_server_common + files('server/linux_main.c'),
  include_directories: kvs_inc,
  dependencies: kvs_deps,
)

kvsbench = executable(
  'kvsbench',
  kvs_bench_common + files('bench/linux_main.c'),
  include_directories: kvs_inc,
  dependencies: kvs_deps,
)

alias_target('kvs', [flexkvs, kvsbench])
alias_target('kvs_linux', [flexkvs, kvsbench])

if cham_enabled
  kvs_cham_deps = kvs_deps + [cham_deps]

  flexkvs_cham = executable(
    'flexkvs_cham',
    kvs_server_common + files('server/cham_main.c'),
    include_directories: kvs_inc,
    dependencies: kvs_cham_deps,
  )

  kvsbench_cham = executable(
    'kvsbench_cham',
    kvs_bench_common + files('bench/cham_main.c'),
    include_directories: kvs_inc,
    dependencies: kvs_cham_deps,
  )

  alias_target('kvs_cham', [flexkvs_cham, kvsbench_cham])
endif

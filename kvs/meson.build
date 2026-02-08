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
  kvs_server_common + files('server/main.c'),
  include_directories: kvs_inc,
  dependencies: kvs_deps,
)

kvsbench = executable(
  'kvsbench',
  kvs_bench_common + files('bench/main.c'),
  include_directories: kvs_inc,
  dependencies: kvs_deps,
)

alias_target('kvs', [flexkvs, kvsbench])
alias_target('kvs_linux', [flexkvs, kvsbench])

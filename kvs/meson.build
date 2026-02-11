kvs_inc = include_directories('common/include')
utils_inc = include_directories('../utils')
clock_util_src = files('../utils/clock.c')
sock_util_src = files('../utils/sock.c')

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

kvsbench_linux = executable(
  'kvsbench_linux',
  kvs_bench_common + files('bench/linux_main.c') + clock_util_src + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
flexkvs_linux = executable(
  'flexkvs_linux',
  kvs_server_common + files('server/linux_main.c') + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
alias_target('kvs_linux', [kvsbench_linux, flexkvs_linux])

if cham_enabled
  kvsbench_cham = executable(
    'kvsbench_cham',
    kvs_bench_common + files('bench/cham_main.c') + clock_util_src + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies: [kvs_deps, cham_deps],
  )
  flexkvs_cham = executable(
    'flexkvs_cham',
    kvs_server_common + files('server/linux_main.c') + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies : [kvs_deps, cham_deps],
  )
  alias_target('kvs_cham', [kvsbench_cham, flexkvs_cham])
endif

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

kvsbench_udp_linux = executable(
  'kvsbench_udp_linux',
  kvs_bench_common + files('bench/udp_linux_main.c') + clock_util_src + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
flexkvs_udp_linux = executable(
  'flexkvs_udp_linux',
  kvs_server_common + files('server/udp_linux_main.c') + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
kvsbench_tcp_linux = executable(
  'kvsbench_tcp_linux',
  kvs_bench_common + files('bench/tcp_linux_main.c') + clock_util_src + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
flexkvs_tcp_linux = executable(
  'flexkvs_tcp_linux',
  kvs_server_common + files('server/tcp_linux_main.c') + sock_util_src,
  include_directories: [kvs_inc, utils_inc],
  dependencies: kvs_deps,
)
alias_target('kvs_udp_linux', [kvsbench_udp_linux, flexkvs_udp_linux])
alias_target('kvs_tcp_linux', [kvsbench_tcp_linux, flexkvs_tcp_linux])

if cham_enabled
  kvsbench_udp_cham = executable(
    'kvsbench_udp_cham',
    kvs_bench_common + files('bench/udp_cham_main.c') + clock_util_src + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies: [kvs_deps, cham_deps],
  )
  flexkvs_udp_cham = executable(
    'flexkvs_udp_cham',
    kvs_server_common + files('server/udp_cham_main.c') + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies : [kvs_deps, cham_deps],
  )
  alias_target('kvs_udp_cham', [kvsbench_udp_cham, flexkvs_udp_cham])
endif

if cham_tcp_enabled
  kvsbench_tcp_cham = executable(
    'kvsbench_tcp_cham',
    kvs_bench_common + files('bench/tcp_cham_main.c') + clock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies: [kvs_deps, cham_tcp_deps],
  )
  flexkvs_tcp_cham = executable(
    'flexkvs_tcp_cham',
    kvs_server_common + files('server/tcp_cham_main.c'),
    include_directories: [kvs_inc, utils_inc],
    dependencies : [kvs_deps, cham_tcp_deps],
  )
  alias_target('kvs_tcp_cham', [kvsbench_tcp_cham, flexkvs_tcp_cham])
endif

if demi_enabled
  kvsbench_udp_demi = executable(
    'kvsbench_udp_demi',
    kvs_bench_common + files('bench/udp_demi_main.c') + clock_util_src + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies: [kvs_deps, demi_dep],
  )
  flexkvs_udp_demi = executable(
    'flexkvs_udp_demi',
    kvs_server_common + files('server/udp_demi_main.c') + sock_util_src,
    include_directories: [kvs_inc, utils_inc],
    dependencies: [kvs_deps, demi_dep],
  )
  alias_target('kvs_udp_demi', [kvsbench_udp_demi, flexkvs_udp_demi])
endif

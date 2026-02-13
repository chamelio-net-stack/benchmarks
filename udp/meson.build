udp_clock_src = files('../utils/clock.c')
udp_hist_src = files('../utils/hist.c')
udp_core_src = files('../utils/core.c')

# cham_deps = [
#   dependency('cham_udp', required: true)
# ]

udp_linux_client = executable(
  'udp_linux_client',
  'udp_linux_client.c',
  udp_clock_src,
  udp_hist_src,
  dependencies: [threads_dep],
)
udp_linux_server = executable(
  'udp_linux_server',
  'udp_linux_server.c',
  udp_clock_src,
  dependencies: [threads_dep],
)
alias_target('udp_linux', [udp_linux_client, udp_linux_server])

if cham_enabled
  udp_cham_client = executable(
    'udp_cham_client',
    'udp_cham_client.c',
    udp_clock_src,
    udp_hist_src,
    udp_core_src,
    dependencies: [threads_dep, cham_deps],
  )
  udp_cham_server = executable(
    'udp_cham_server',
    'udp_cham_server.c',
    udp_clock_src,
    udp_core_src,
    dependencies : [threads_dep, cham_deps],
  )
  alias_target('udp_cham', [udp_cham_client, udp_cham_server])
endif

if demi_enabled
  udp_demi_client = executable(
    'udp_demi_client',
    'udp_demi_client.c',
    udp_clock_src,
    udp_hist_src,
    dependencies: [threads_dep, demi_dep],
  )
  udp_demi_server = executable(
    'udp_demi_server',
    'udp_demi_server.c',
    udp_clock_src,
    dependencies : [threads_dep, demi_dep],
  )
  alias_target('udp_cham', [udp_demi_client, udp_demi_server])
endif

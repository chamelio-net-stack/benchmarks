tcp_clock_src = files('../utils/clock.c')
tcp_hist_src = files('../utils/hist.c')
tcp_core_src = files('../utils/core.c')

tcp_linux_client = executable(
  'tcp_linux_client',
  'tcp_linux_client.c',
  tcp_clock_src,
  tcp_hist_src,
  dependencies: [threads_dep],
)
tcp_linux_server = executable(
  'tcp_linux_server',
  'tcp_linux_server.c',
  tcp_clock_src,
  dependencies: [threads_dep],
)
alias_target('tcp_linux', [tcp_linux_client, tcp_linux_server])

tcp_etran_client = executable(
  'tcp_etran_client',
  'tcp_linux_client.c',
  tcp_clock_src,
  tcp_hist_src,
  # eTRAN interposes fcntl, but not Meson's large-file fcntl64 alias.
  c_args: ['-DETRAN', '-U_FILE_OFFSET_BITS'],
  dependencies: [threads_dep],
)
tcp_etran_server = executable(
  'tcp_etran_server',
  'tcp_linux_server.c',
  tcp_clock_src,
  c_args: ['-DETRAN', '-U_FILE_OFFSET_BITS'],
  dependencies: [threads_dep],
)
alias_target('tcp_etran', [tcp_etran_client, tcp_etran_server])

if cham_tcp_enabled
  tcp_cham_client = executable(
    'tcp_cham_client',
    'tcp_cham_client.c',
    tcp_clock_src,
    tcp_hist_src,
    tcp_core_src,
    dependencies: [threads_dep, math_dep, cham_tcp_deps],
  )
  tcp_cham_server = executable(
    'tcp_cham_server',
    'tcp_cham_server.c',
    tcp_clock_src,
    tcp_core_src,
    dependencies: [threads_dep, cham_tcp_deps],
  )
  alias_target('tcp_cham', [tcp_cham_client, tcp_cham_server])
endif

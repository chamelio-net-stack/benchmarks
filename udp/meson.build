# cham_deps = [
#   dependency('cham_udp', required: true)
# ]

# executable(
#   'udp_linux_client',
#   'udp_linux_client.c'
# )

# executable(
#   'udp_linux_server',
#   'udp_linux_server.c'
# )

# executable(
#   'udp_cham_client',
#   'udp_cham_client.c',
#   dependencies: cham_deps,
# )

# executable(
#   'udp_cham_server',
#   'udp_cham_server.c',
#   dependencies : cham_deps,
# )

executable(
  'udp_demi_client',
  'udp_demi_client.c',
  dependencies: [demi_dep],
)

executable(
  'udp_demi_server',
  'udp_demi_server.c',
  dependencies : [demi_dep],
)

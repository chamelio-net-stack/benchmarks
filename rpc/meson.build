cham_rpc_inc = include_directories('/workspaces/chamelio/protos/rpc/include')
rpc_core_src = files('../utils/core.c')
rpc_clock_src = files('../utils/clock.c')

rpc_cham_client = executable(
    'rpc_cham_client',
    'rpc_cham_client.c',
    dependencies: cham_deps,
)
rpc_cham_server = executable(
    'rpc_cham_server',
    'rpc_cham_server.c',
    rpc_core_src,
    rpc_clock_src,
    dependencies: cham_deps,
    link_args: ['-lm'],
)
alias_target('rpc_cham', [rpc_cham_client, rpc_cham_server])
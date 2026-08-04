cham_rpc_inc = include_directories('/workspaces/chamelio/protos/rpc/include')

rpc_cham_client = executable(
    'rpc_cham_client',
    'rpc_cham_client.c',
    dependencies: cham_deps,
)
rpc_cham_server = executable(
    'rpc_cham_server',
    'rpc_cham_server.c',
    dependencies: cham_deps,
)
alias_target('rpc_cham', [rpc_cham_client, rpc_cham_server])
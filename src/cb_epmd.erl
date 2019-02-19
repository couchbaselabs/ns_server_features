-module(cb_epmd).

%% External exports
-export([start/0, start_link/0, stop/0, port_for_node/2, port_please/2,
         port_please/3, names/0, names/1,
         register_node/2, register_node/3, is_local_node/1]).


%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

%% Starting erl_epmd only for backward compat
%% Old clusters will use epmd to discover this nodes. When upgrade is finished
%% epmd is not needed.
start() -> erl_epmd:start().
start_link() -> erl_epmd:start_link().
stop() -> erl_epmd:stop().

%% Node here comes without hostname and as string (not 'n_1@127.0.0.1 but "n_1")
port_please(Node, Hostname) ->
    port_please(Node, Hostname, infinity).

port_please(NodeStr, Hostname, Timeout) ->
    Module = cb_dist:get_prefered_dist(NodeStr),
    case node_type(NodeStr) of
        %% needed for backward compat: old ns_server nodes use dynamic ports
        %% so the only way to know those ports is to ask real epmd
        %% for this reason we also keep registering new static ports on epmd
        %% because old nodes doesn't know anything about those ports
        {ok, ns_server, _} when Module == inet_tcp_dist;
                                Module == inet6_tcp_dist ->
            erl_epmd:port_please(NodeStr, Hostname, Timeout);
        {ok, Type, N} ->
            {port, port(Type, N, Module), 5};
        {error, Reason} ->
            {error, Reason}
    end.

names() -> erl_epmd:names().

names(EpmdAddr) -> erl_epmd:names(EpmdAddr).

register_node(Name, PortNo) ->
    register_node(Name, PortNo, inet).

register_node(Name, PortNo, Family) ->
    %% backward compat: we need to register non tls ports on epmd to allow
    %% old nodes to find this node.
    %% When upgrade is finished this code can be dropped
    case is_ns_server_non_tls_port(PortNo) of
        true ->
            %% creation is zero because we don't use it anyway
            %% real 'creation' is generated in cb_dist.erl
            case erl_epmd:register_node(Name, PortNo, Family) of
                {ok, _} -> {ok, 0};
                {error, already_registered} -> {ok, 0}
            end;
        false -> {ok, 0}
    end.

port_for_node(Module, NodeStr) ->
    case node_type(NodeStr) of
        {ok, Type, N} ->
            {ok, port(Type, N, Module)};
        {error, Reason} ->
            {error, Reason}
    end.

is_local_node(Node) when is_atom(Node) -> is_local_node(atom_to_list(Node));
is_local_node(Node) ->
    [NodeName | _] = string:tokens(Node, "@"),
    case node_type(NodeName) of
        {ok, ns_server, _} -> false;
        {ok, _, _} -> true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

port(Type, N, Module) ->
    base_port(Type) + list_to_integer(N) * 2 + shift(Module).

port_shifts() ->
    [{inet_tcp_dist,  0},
     {inet6_tcp_dist, 0},
     {inet_tls_dist,  1},
     {inet6_tls_dist, 1}].

shift(Module) -> proplists:get_value(Module, port_shifts()).

base_port(ns_server) -> 21100;
base_port(babysitter) -> 21200;
base_port(couchdb) -> 21300.

%% it's magic but it's needed for backward compat only
is_ns_server_non_tls_port(Port) ->
    (base_port(ns_server) =< Port) andalso
    (Port < base_port(babysitter)) andalso
    (((Port - base_port(ns_server)) rem 2) == 0).

node_type("ns_1") -> {ok, ns_server, 0};
node_type("babysitter_of_ns_1") -> {ok, babysitter, 0};
node_type("couchdb_ns_1") -> {ok, couchdb, 0};

node_type("n_" ++ Nstr) -> {ok, ns_server, Nstr};
node_type("babysitter_of_n_" ++ Nstr) -> {ok, babysitter, Nstr};
node_type("couchdb_n_" ++ Nstr) -> {ok, couchdb, Nstr};

node_type(Name) -> {error, {unknown_node, Name}}.

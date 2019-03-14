%% @author Couchbase <info@couchbase.com>
%% @copyright 2017-2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

%% @doc implementation of node related REST API's

-module(menelaus_web_node).

-include("ns_common.hrl").
-include("cut.hrl").
-include_lib("kernel/include/net_address.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([handle_node/2,
         build_full_node_info/2,
         build_memory_quota_info/1,
         build_nodes_info_fun/4,
         build_nodes_info/4,
         build_node_hostname/3,
         handle_bucket_node_list/2,
         handle_bucket_node_info/3,
         find_node_hostname/2,
         handle_node_statuses/1,
         handle_node_rename/1,
         handle_node_altaddr_external/1,
         handle_node_altaddr_external_delete/1,
         handle_node_self_xdcr_ssl_ports/1,
         handle_node_settings_post/2,
         apply_node_settings/1,
         alternate_addresses_json/3,
         handle_setup_net_config/1,
         handle_dist_protocols/1,
         apply_net_config/2,
         apply_ext_dist_protocols/1]).

-import(menelaus_util,
        [local_addr/1,
         reply_json/2,
         reply_json/3,
         bin_concat_path/1,
         reply_not_found/1,
         reply/2]).

handle_node("self", Req) ->
    handle_node(node(), Req);
handle_node(S, Req) when is_list(S) ->
    handle_node(list_to_atom(S), Req);
handle_node(Node, Req) when is_atom(Node) ->
    LocalAddr = local_addr(Req),
    case lists:member(Node, ns_node_disco:nodes_wanted()) of
        true ->
            Result = build_full_node_info(Node, LocalAddr),
            reply_json(Req, Result);
        false ->
            reply_json(Req, <<"Node is unknown to this cluster.">>, 404)
    end.

% S = [{ssd, []},
%      {hdd, [[{path, /some/nice/disk/path}, {quotaMb, 1234}, {state, ok}],
%            [{path, /another/good/disk/path}, {quotaMb, 5678}, {state, ok}]]}].
%
storage_conf_to_json(S) ->
    lists:map(fun ({StorageType, Locations}) -> % StorageType is ssd or hdd.
                  {StorageType, lists:map(fun (LocationPropList) ->
                                              {struct, lists:map(fun location_prop_to_json/1, LocationPropList)}
                                          end,
                                          Locations)}
              end,
              S).

location_prop_to_json({path, L}) -> {path, list_to_binary(L)};
location_prop_to_json({index_path, L}) -> {index_path, list_to_binary(L)};
location_prop_to_json({cbas_dirs, L}) -> {cbas_dirs, [list_to_binary(El) || El <- L]};
location_prop_to_json({java_home, undefined}) -> {java_home, <<>>};
location_prop_to_json({java_home, L}) -> {java_home, list_to_binary(L)};
location_prop_to_json({quotaMb, none}) -> {quotaMb, none};
location_prop_to_json({state, ok}) -> {state, ok};
location_prop_to_json(KV) -> KV.

build_full_node_info(Node, LocalAddr) ->
    {struct, KV} = (build_nodes_info_fun(true, normal, unstable, LocalAddr))(Node, undefined),
    NodeStatus = ns_doctor:get_node(Node),
    StorageConf = ns_storage_conf:storage_conf_from_node_status(Node, NodeStatus),
    R = {struct, storage_conf_to_json(StorageConf)},
    DiskData = proplists:get_value(disk_data, NodeStatus, []),

    Fields = [{availableStorage, {struct, [{hdd, [{struct, [{path, list_to_binary(Path)},
                                                            {sizeKBytes, SizeKBytes},
                                                            {usagePercent, UsagePercent}]}
                                                  || {Path, SizeKBytes, UsagePercent} <- DiskData]}]}},
              {storageTotals, {struct, [{Type, {struct, PropList}}
                                        || {Type, PropList} <- ns_storage_conf:nodes_storage_info([Node])]}},
              {storage, R}] ++ KV ++ build_memory_quota_info(ns_config:latest()),
    {struct, lists:filter(fun (X) -> X =/= undefined end,
                                   Fields)}.

build_memory_quota_info(Config) ->
    CompatVersion = cluster_compat_mode:get_compat_version(Config),
    lists:map(
      fun (Service) ->
              {ok, Quota} = memory_quota:get_quota(Config, Service),
              {memory_quota:service_to_json_name(Service), Quota}
      end, memory_quota:aware_services(CompatVersion)).

build_nodes_info(CanIncludeOtpCookie, InfoLevel, Stability, LocalAddr) ->
    F = build_nodes_info_fun(CanIncludeOtpCookie, InfoLevel, Stability, LocalAddr),
    [F(N, undefined) || N <- ns_node_disco:nodes_wanted()].

%% builds health/warmup status of given node (w.r.t. given Bucket if
%% not undefined)
build_node_status(Node, Bucket, InfoNode, BucketsAll) ->
    case proplists:get_bool(down, InfoNode) of
        false ->
            ReadyBuckets = proplists:get_value(ready_buckets, InfoNode),
            NodeBucketNames = ns_bucket:node_bucket_names(Node, BucketsAll),
            case Bucket of
                undefined ->
                    case ordsets:is_subset(lists:sort(NodeBucketNames),
                                           lists:sort(ReadyBuckets)) of
                        true ->
                            <<"healthy">>;
                        false ->
                            <<"warmup">>
                    end;
                _ ->
                    case lists:member(Bucket, ReadyBuckets) of
                        true ->
                            <<"healthy">>;
                        false ->
                            case lists:member(Bucket, NodeBucketNames) of
                                true ->
                                    <<"warmup">>;
                                false ->
                                    <<"unhealthy">>
                            end
                    end
            end;
        true ->
            <<"unhealthy">>
    end.

build_nodes_info_fun(CanIncludeOtpCookie, InfoLevel, Stability, LocalAddr) ->
    OtpCookie = list_to_binary(atom_to_list(erlang:get_cookie())),
    NodeStatuses = ns_doctor:get_nodes(),
    Config = ns_config:get(),
    BucketsAll = ns_bucket:get_buckets(Config),
    fun(WantENode, Bucket) ->
            InfoNode = ns_doctor:get_node(WantENode, NodeStatuses),
            KV = build_node_info(Config, WantENode, InfoNode, LocalAddr),

            Status = build_node_status(WantENode, Bucket, InfoNode, BucketsAll),
            KV1 = [{clusterMembership,
                    atom_to_binary(
                      ns_cluster_membership:get_cluster_membership(
                        WantENode, Config),
                      latin1)},
                   {recoveryType,
                    ns_cluster_membership:get_recovery_type(Config, WantENode)},
                   {status, Status},
                   {otpNode, list_to_binary(atom_to_list(WantENode))}
                   | KV],
            %% NOTE: the following avoids exposing otpCookie to UI
            KV2 = case CanIncludeOtpCookie andalso InfoLevel =:= normal of
                      true ->
                          [{otpCookie, OtpCookie} | KV1];
                      false -> KV1
                  end,
            KV3 = case Bucket of
                      undefined ->
                          [{Key, URL} || {Key, Node} <- [{couchApiBase, WantENode},
                                                         {couchApiBaseHTTPS, {ssl, WantENode}}],
                                         URL <- [capi_utils:capi_url_bin(Node, <<"/">>, LocalAddr)],
                                         URL =/= undefined] ++ KV2;
                      _ ->
                          Replication = case ns_bucket:get_bucket(Bucket, Config) of
                                            not_present -> 0.0;
                                            {ok, BucketConfig} ->
                                                failover_safeness_level:extract_replication_uptodateness(Bucket, BucketConfig,
                                                                                                         WantENode, NodeStatuses)
                                        end,
                          [{replication, Replication} | KV2]
                  end,
            KV4 = case Stability of
                      stable ->
                          KV3;
                      unstable ->
                          build_extra_node_info(Config, WantENode,
                                                InfoNode, BucketsAll, KV3)
                  end,
            {struct, KV4}
    end.

build_extra_node_info(Config, Node, InfoNode, _BucketsAll, Append) ->

    {UpSecs, {MemoryTotalErlang, MemoryAllocedErlang, _}} =
        {proplists:get_value(wall_clock, InfoNode, 0),
         proplists:get_value(memory_data, InfoNode,
                             {0, 0, undefined})},

    SystemStats = proplists:get_value(system_stats, InfoNode, []),
    SigarMemTotal = proplists:get_value(mem_total, SystemStats),
    SigarMemFree = proplists:get_value(mem_free, SystemStats),
    {MemoryTotal, MemoryFree} =
        case SigarMemTotal =:= undefined orelse SigarMemFree =:= undefined of
            true ->
                {MemoryTotalErlang, MemoryTotalErlang - MemoryAllocedErlang};
            _ ->
                {SigarMemTotal, SigarMemFree}
        end,

    NodesBucketMemoryTotal = case ns_config:search_node_prop(Node,
                                                             Config,
                                                             memcached,
                                                             max_size) of
                                 X when is_integer(X) -> X;
                                 undefined -> (MemoryTotal * 4) div (5 * ?MIB)
                             end,

    NodesBucketMemoryAllocated = NodesBucketMemoryTotal,
    [{systemStats, {struct, proplists:get_value(system_stats, InfoNode, [])}},
     {interestingStats, {struct, proplists:get_value(interesting_stats, InfoNode, [])}},
     %% TODO: deprecate this in API (we need 'stable' "startupTStamp"
     %% first)
     {uptime, list_to_binary(integer_to_list(UpSecs))},
     %% TODO: deprecate this in API
     {memoryTotal, erlang:trunc(MemoryTotal)},
     %% TODO: deprecate this in API
     {memoryFree, erlang:trunc(MemoryFree)},
     %% TODO: deprecate this in API
     {mcdMemoryReserved, erlang:trunc(NodesBucketMemoryTotal)},
     %% TODO: deprecate this in API
     {mcdMemoryAllocated, erlang:trunc(NodesBucketMemoryAllocated)}
     | Append].

build_node_hostname(Config, Node, LocalAddr) ->
    H = misc:extract_node_address(Node),
    Host = case misc:is_localhost(H) of
               true  -> LocalAddr;
               false -> H
           end,
    misc:join_host_port(Host, service_ports:get_port(rest_port, Config, Node)).

alternate_addresses_json(Node, Config, WantedPorts) ->
    {ExtHostname, ExtPorts} =
        service_ports:get_external_host_and_ports(
          Node, Config, WantedPorts),
    External = construct_ext_json(ExtHostname, ExtPorts),
    [{alternateAddresses, {struct, External}} || External =/= []].

construct_ext_json(undefined, _Ports) ->
    [];
construct_ext_json(Hostname, []) ->
    [{external, {struct, [{hostname, list_to_binary(Hostname)}]}}];
construct_ext_json(Hostname, Ports) ->
    [{external, {struct, [{hostname, list_to_binary(Hostname)},
                          {ports, {struct, Ports}}]}}].

is_xdcr_over_ssl_allowed() ->
    cluster_compat_mode:is_enterprise().

build_node_info(Config, WantENode, InfoNode, LocalAddr) ->
    Versions = proplists:get_value(version, InfoNode, []),
    Version = proplists:get_value(ns_server, Versions, "unknown"),
    OS = proplists:get_value(system_arch, InfoNode, "unknown"),
    CpuCount = proplists:get_value(cpu_count, InfoNode, unknown),
    HostName = build_node_hostname(Config, WantENode, LocalAddr),
    NodeUUID = ns_config:search_node_with_default(WantENode, Config, uuid,
                                                  undefined),

    PortKeys = [{memcached_port, direct},
                %% this is used by xdcr over ssl since 2.5.0
                {ssl_capi_port, httpsCAPI},
                {ssl_rest_port, httpsMgmt}],

    PortsKV = lists:filtermap(
                fun ({Key, JKey}) ->
                        case service_ports:get_port(Key, Config, WantENode) of
                            undefined ->
                                false;
                            Value ->
                                {true, {JKey, Value}}
                        end
                end, PortKeys),

    WantedPorts = [memcached_port,
                   ssl_capi_port,
                   capi_port,
                   ssl_rest_port,
                   rest_port],

    AFamily = ns_config:search_node_with_default(Config, address_family, inet),
    CEncryption = ns_config:search_node_with_default(Config, cluster_encryption,
                                                     false),
    Listeners = ns_config:search_node_with_default(Config,
                                                   erl_external_dist_protocols,
                                                   undefined),

    RV = [{hostname, list_to_binary(HostName)},
          {nodeUUID, NodeUUID},
          {clusterCompatibility, cluster_compat_mode:effective_cluster_compat_version()},
          {version, list_to_binary(Version)},
          {os, list_to_binary(OS)},
          {cpuCount, CpuCount},
          {ports, {struct, PortsKV}},
          {services, ns_cluster_membership:node_services(Config, WantENode)},
          {address_family, AFamily},
          {cluster_encryption, CEncryption},
          {dist_protocols, Listeners}
         ] ++ alternate_addresses_json(WantENode, Config, WantedPorts),
    case WantENode =:= node() of
        true ->
            [{thisNode, true} | RV];
        _ -> RV
    end.

nodes_to_hostnames(Config, Req) ->
    Nodes = ns_cluster_membership:active_nodes(Config),
    LocalAddr = local_addr(Req),
    [{N, list_to_binary(build_node_hostname(Config, N, LocalAddr))}
     || N <- Nodes].

%% Node list
%% GET /pools/default/buckets/{Id}/nodes
%%
%% Provides a list of nodes for a specific bucket (generally all nodes) with
%% links to stats for that bucket
handle_bucket_node_list(BucketName, Req) ->
    %% NOTE: since 4.0 release we're listing all active nodes as
    %% part of our approach for dealing with query stats
    NHs = nodes_to_hostnames(ns_config:get(), Req),
    Servers =
        [{struct,
          [{hostname, Hostname},
           {uri, bin_concat_path(["pools", "default", "buckets", BucketName, "nodes", Hostname])},
           {stats, {struct, [{uri,
                              bin_concat_path(
                                ["pools", "default", "buckets", BucketName, "nodes", Hostname, "stats"])}]}}]}
         || {_, Hostname} <- NHs],
    reply_json(Req, {struct, [{servers, Servers}]}).

find_node_hostname(HostnameList, Req) ->
    Hostname = list_to_binary(HostnameList),
    NHs = nodes_to_hostnames(ns_config:get(), Req),
    case [N || {N, CandidateHostname} <- NHs,
               CandidateHostname =:= Hostname] of
        [] ->
            false;
        [Node] ->
            {ok, Node}
    end.

%% Per-Node Stats URL information
%% GET /pools/default/buckets/{Id}/nodes/{NodeId}
%%
%% Provides node hostname and links to the default bucket and node-specific
%% stats for the default bucket
%%
%% TODO: consider what else might be of value here
handle_bucket_node_info(BucketName, Hostname, Req) ->
    case find_node_hostname(Hostname, Req) of
        false ->
            reply_not_found(Req);
        _ ->
            BucketURI = bin_concat_path(["pools", "default", "buckets", BucketName]),
            NodeStatsURI = bin_concat_path(
                             ["pools", "default", "buckets", BucketName, "nodes", Hostname, "stats"]),
            reply_json(Req,
                       {struct, [{hostname, list_to_binary(Hostname)},
                                 {bucket, {struct, [{uri, BucketURI}]}},
                                 {stats, {struct, [{uri, NodeStatsURI}]}}]})
    end.

average_failover_safenesses(Node, NodeInfos, BucketsAll) ->
    average_failover_safenesses_rec(Node, NodeInfos, BucketsAll, 0, 0).

average_failover_safenesses_rec(_Node, _NodeInfos, [], Sum, Count) ->
    try Sum / Count
    catch error:badarith -> 1.0
    end;
average_failover_safenesses_rec(Node, NodeInfos, [{BucketName, BucketConfig} | RestBuckets], Sum, Count) ->
    Level = failover_safeness_level:extract_replication_uptodateness(BucketName, BucketConfig, Node, NodeInfos),
    average_failover_safenesses_rec(Node, NodeInfos, RestBuckets, Sum + Level, Count + 1).

%% this serves fresh nodes replication and health status
handle_node_statuses(Req) ->
    LocalAddr = local_addr(Req),
    OldStatuses = ns_doctor:get_nodes(),
    Config = ns_config:get(),
    BucketsAll = ns_bucket:get_buckets(Config),
    FreshStatuses = ns_heart:grab_fresh_failover_safeness_infos(BucketsAll),
    NodeStatuses =
        lists:map(
          fun (N) ->
                  InfoNode = ns_doctor:get_node(N, OldStatuses),
                  Hostname = proplists:get_value(hostname,
                                                 build_node_info(Config, N, InfoNode, LocalAddr)),
                  NewInfoNode = ns_doctor:get_node(N, FreshStatuses),
                  Dataless = not lists:member(kv, ns_cluster_membership:node_services(Config, N)),
                  V = case proplists:get_bool(down, NewInfoNode) of
                          true ->
                              {struct, [{status, unhealthy},
                                        {otpNode, N},
                                        {dataless, Dataless},
                                        {replication, average_failover_safenesses(N, OldStatuses, BucketsAll)}]};
                          false ->
                              GracefulFailoverPossible =
                                  case ns_rebalancer:check_graceful_failover_possible([N], BucketsAll) of
                                      true -> true;
                                      {false, _} -> false
                                  end,
                              {struct, [{status, healthy},
                                        {gracefulFailoverPossible, GracefulFailoverPossible},
                                        {otpNode, N},
                                        {dataless, Dataless},
                                        {replication, average_failover_safenesses(N, FreshStatuses, BucketsAll)}]}
                      end,
                  {Hostname, V}
          end, ns_node_disco:nodes_wanted()),
    reply_json(Req, {struct, NodeStatuses}, 200).

handle_node_rename(Req) ->
    Params = mochiweb_request:parse_post(Req),
    Node = node(),

    Reply =
        case proplists:get_value("hostname", Params) of
            undefined ->
                {error, <<"The name cannot be empty">>, 400};
            Hostname ->
                case ns_cluster:change_address(Hostname) of
                    ok ->
                        ns_audit:rename_node(Req, Node, Hostname),
                        ok;
                    not_renamed ->
                        ok;
                    {cannot_resolve, Errno} ->
                        Msg = io_lib:format("Could not resolve the hostname: ~p", [Errno]),
                        {error, iolist_to_binary(Msg), 400};
                    {cannot_listen, Errno} ->
                        Msg = io_lib:format("Could not listen: ~p", [Errno]),
                        {error, iolist_to_binary(Msg), 400};
                    not_self_started ->
                        Msg = <<"Could not rename the node because name was fixed at server start-up.">>,
                        {error, Msg, 403};
                    {address_save_failed, E} ->
                        Msg = io_lib:format("Could not save address after rename: ~p", [E]),
                        {error, iolist_to_binary(Msg), 500};
                    {address_not_allowed, Message} ->
                        Msg = io_lib:format("Requested name hostname is not allowed: ~s", [Message]),
                        {error, iolist_to_binary(Msg), 400};
                    already_part_of_cluster ->
                        Msg = <<"Renaming is disallowed for nodes that are already part of a cluster">>,
                        {error, Msg, 400}
                end
        end,

    case Reply of
        ok ->
            reply(Req, 200);
        {error, Error, Status} ->
            reply_json(Req, [Error], Status)
    end.

handle_node_self_xdcr_ssl_ports(Req) ->
    case is_xdcr_over_ssl_allowed() of
        false ->
            reply_json(Req, [], 403);
        true ->
            Ports = [{httpsMgmt, service_ports:get_port(ssl_rest_port)},
                     {httpsCAPI, service_ports:get_port(ssl_capi_port)}] ++
                alternate_addresses_json(node(), ns_config:latest(),
                                         [ssl_capi_port, ssl_rest_port]),
            reply_json(Req, {struct, Ports})
    end.

validate_path({java_home, JavaHome}, _) ->
    validate_java_home(JavaHome);
validate_path(PathTuple, DbPath) ->
    validate_ix_cbas_path(PathTuple, DbPath).

validate_java_home([]) ->
    false;
validate_java_home(not_changed) ->
    false;
validate_java_home(JavaHome) ->
    case misc:run_external_tool(
           path_config:component_path(bin, "cbas"),
           ["-validateJavaHome", "-javaHome", JavaHome], []) of
        {0, _} ->
            false;
        {Code, Output} ->
            ?log_debug("Java home validation of ~p failed with code=~p, ~p",
                       [JavaHome, Code, Output]),
            {true, iolist_to_binary(
                     io_lib:format(
                       "'~s' has incorrect version of java or is not a "
                       "java home directory", [JavaHome]))}
    end.

validate_ix_cbas_path({_Param, DbPath}, DbPath) ->
    false;
validate_ix_cbas_path({Param, Path}, DbPath) ->
    PathTokens = filename:split(Path),
    DbPathTokens = filename:split(DbPath),
    case lists:prefix(DbPathTokens, PathTokens) of
        true ->
            {true, iolist_to_binary(
                      io_lib:format("'~p' (~s) must not be a sub-directory "
                                    "of 'data_path' (~s)",
                                    [Param, Path, DbPath]))};
        false -> false
    end.

validate_and_expand_path(java_home, []) ->
    {ok, {java_home, []}};
validate_and_expand_path(java_home, not_changed) ->
    {ok, {java_home, not_changed}};
validate_and_expand_path(Field, []) ->
    {error, iolist_to_binary(
              io_lib:format("~p cannot contain empty string", [Field]))};
validate_and_expand_path(Field, Path) ->
    case misc:is_absolute_path(Path) of
        true ->
            case misc:realpath(Path, "/") of
                {ok, RP} ->
                    {ok, {Field, RP}};
                {error, _, ExpandedSubPath, RemPathComps, {error, enoent}} ->
                    %% We get here if a sub-directory hierarchy is not present
                    %% in the path. We create a path using the expanded sub-path
                    %% and the remaining path components.
                    NewPath = filename:join([ExpandedSubPath | RemPathComps]),
                    {ok, {Field, NewPath}};
                Err ->
                    {error,
                     iolist_to_binary(
                      io_lib:format("Path expansion failed for ~p: ~p",
                                    [Field, Err]))}
            end;
        false ->
            {error, iolist_to_binary(
                      io_lib:format("An absolute path is required for ~p",
                                    [Field]))}
    end.

-spec handle_node_settings_post(string() | atom(), any()) -> no_return().
handle_node_settings_post("self", Req) ->
    handle_node_settings_post(node(), Req);
handle_node_settings_post(NodeStr, Req) when is_list(NodeStr) ->
    try list_to_existing_atom(NodeStr) of
        Node -> handle_node_settings_post(Node, Req)
    catch
        error:badarg -> menelaus_util:reply_not_found(Req)
    end;
handle_node_settings_post(Node, Req) when is_atom(Node) ->
    case cluster_compat_mode:is_cluster_madhatter() of
        false when Node =/= node() ->
            throw({web_exception, 400,
                   <<"Setting the disk storage path for other servers is "
                     "not yet supported.">>, []});
        _ ->
            ok
    end,

    Params = mochiweb_request:parse_post(Req),

    case lists:member(Node, ns_node_disco:nodes_actual())  of
        true ->
            %% NOTE: due to required restart we need to protect
            %%       ourselves from 'death signal' of parent
            Node == node() andalso erlang:process_flag(trap_exit, true),

            case remote_api:apply_node_settings(Node, Params) of
                not_changed ->
                    reply(Req, 200);
                ok  ->
                    ns_audit:disk_storage_conf(Req, Node, Params),
                    reply(Req, 200);
                {error, Msgs} ->
                    reply_json(Req, Msgs, 400)
            end,

            %% NOTE: we have to stop this process because in case of
            %%       ns_server restart it becomes orphan
            Node == node() andalso erlang:exit(normal);
        false ->
            case lists:member(Node, ns_node_disco:nodes_wanted()) of
                true ->
                    menelaus_util:reply_text(Req, "Node is not available.",
                                             503);
                false ->
                    menelaus_util:reply_not_found(Req)
            end
    end.

-spec apply_node_settings(Params :: [{Key :: string(), Value :: term()}]) ->
        ok | not_changed | {error, [Msg :: binary()]}.
apply_node_settings(Params) ->
    try
        Paths = validate_settings_paths(extract_settings_paths(Params)),

        DbPath = proplists:get_value(path, Paths),
        IxPath = proplists:get_value(index_path, Paths),
        CBASDirs = proplists:get_all_values(cbas_path, Paths),
        JavaHome = proplists:get_value(java_home, Paths),

        {ok, DefaultDbPath} = ns_storage_conf:this_node_dbdir(),
        DbPathChanged = DbPath =/= DefaultDbPath,
        Provisioned = ns_config_auth:is_system_provisioned(),
        NodeStatus = ns_cluster_membership:get_cluster_membership(node()),

        case DbPathChanged and Provisioned and (NodeStatus =/= inactiveAdded) of
            true ->
                %% MB-7344: we had 1.8.1 instructions allowing that. And
                %% 2.0 works very differently making that original
                %% instructions lose data. Thus we decided it's much safer
                %% to un-support this path.
                Msg = "Changing data of nodes that are part of provisioned "
                      "cluster is not supported",
                erlang:throw({error, [Msg]});
            _ ->
                ok
        end,

        RV1 =
            case ns_storage_conf:setup_disk_storage_conf(DbPath, IxPath,
                                                         CBASDirs) of
                restart ->
                    %% performing required restart from
                    %% successfull path change
                    {ok, _} = ns_server_cluster_sup:restart_ns_server(),
                    ok;
                Other ->
                    Other
            end,

        RV2 =
            case RV1 of
                {errors, _} ->
                    RV1;
                _ ->
                    {RV1, ns_storage_conf:update_java_home(JavaHome)}
            end,

        case RV2 of
            {not_changed, not_changed} ->
                not_changed;
            {errors, Errors} ->
                {error, Errors};
            _ ->
                ok
        end
    catch
        throw:{error, ErrorMsgs} ->
            {error, [iolist_to_binary(M) || M <- ErrorMsgs]}
    end.

extract_settings_paths(Params) ->
    {ok, DefaultDbPath} = ns_storage_conf:this_node_dbdir(),
    {ok, DefaultIxPath} = ns_storage_conf:this_node_ixdir(),

    CBASDirsToSet =
        case [Dir || {"cbas_path", Dir} <- Params] of
            [] -> ns_storage_conf:this_node_cbas_dirs();
            Dirs -> Dirs
        end,

    [{path, proplists:get_value("path", Params, DefaultDbPath)},
     {index_path, proplists:get_value("index_path", Params, DefaultIxPath)},
     {java_home, proplists:get_value("java_home", Params, not_changed)}] ++
        [{cbas_path, P} || P <- CBASDirsToSet].

validate_settings_paths(Paths) ->
    ValidateRes = [validate_and_expand_path(K, V) || {K, V} <- Paths],
    ValidationErrors = [E || {error, E} <- ValidateRes],

    ValidationErrors == [] orelse throw({error, ValidationErrors}),

    ResPaths = [P || {ok, P} <- ValidateRes],

    DbPath = proplists:get_value(path, ResPaths),
    ValidationErrors1 =
        lists:filtermap(validate_path(_, DbPath), ResPaths),

    ValidationErrors1 == [] orelse throw({error, ValidationErrors1}),

    ResPaths.

%% Basic port validation is done.
%% The below port validations are not performed.
%%  - Verify if all ports being setup for "external" have their particular
%%    service enabled on the node.
%%  - Verify if no two hostname:port pair are the same in a cluster.
%% Reasoning behind not performing above validations is that the node can have
%% "external" addresses configured before it has been added to the cluster, or
%% it's services configured. Therefore, we keep port validation simple and trust
%% the admin to setup "external" addresses correctly for the clients.
parse_validate_ports(Params) ->
    lists:foldl(
      fun ({RestName, Value}, Acc) ->
              try
                  PortKey =
                      case service_ports:find_by_rest_name(RestName) of
                          undefined ->
                              throw({error, [<<"No such port.">>]});
                          P ->
                              P
                      end,
                  Port = menelaus_util:parse_validate_port_number(Value),
                  [{PortKey, Port} | Acc]
              catch
                  throw:{error, [Msg]} ->
                      ErrorMsg = io_lib:format("Invalid Port ~p : ~s",
                                               [RestName, Msg]),
                      throw({web_exception, 400, ErrorMsg, []})
              end
      end, [], Params).

parse_validate_external_params(Params) ->
    Hostname = case proplists:get_value("hostname", Params) of
                   undefined ->
                       throw({web_exception, 400, "hostname should be specified", []});
                   HostNm ->
                       string:trim(HostNm)
               end,
    Ports = parse_validate_ports(proplists:delete("hostname", Params)),
    [{external, [{hostname, Hostname}, {ports, Ports}]}].

%% This replaces any existing alternate_addresses config of this node.
%% For now this is fine because external is only element in alternate_addresses.
handle_node_altaddr_external(Req) ->
    menelaus_util:assert_is_55(),
    Params = mochiweb_request:parse_post(Req),
    External = parse_validate_external_params(Params),
    ns_config:set({node, node(), alternate_addresses}, External),
    menelaus_util:reply(Req, 200).

%% Delete alternate_addresses as external is the only element in
%% alternate_addresses.
handle_node_altaddr_external_delete(Req) ->
    menelaus_util:assert_is_55(),
    ns_config:delete({node, node(), alternate_addresses}),
    menelaus_util:reply(Req, 200).

is_raw_addr_node(Node) ->
    {_, Host} = misc:node_name_host(Node),
    inet:parse_address(Host) =/= {error, einval}.

check_for_raw_addr(State) ->
    %% Fail the request if the cluster is provisioned and has any node
    %% setup with raw IP address.
    case ns_config_auth:is_system_provisioned() of
        true ->
            RawAddrNodes = lists:filter(fun is_raw_addr_node/1,
                                        ns_node_disco:nodes_wanted()),
            case RawAddrNodes of
                [] ->
                    State;
                _ ->
                    M = io_lib:format("Can't change address family when "
                                      "nodes are configured with raw IP "
                                      "addresses: ~p", [RawAddrNodes]),
                    validator:return_error('_', M, State)
            end;
        false ->
            State
    end.

check_if_net_config_allowed(State) ->
    case ns_config_auth:is_system_provisioned() andalso
        auto_failover:is_enabled() of
        true  ->
            M = "Can't change network configuration when auto-failover "
                "is enabled.",
            validator:return_error('_', M, State);
        false ->
            State
    end.

net_config_validators() ->
    [validator:has_params(_),
     validator:required(afamily, _),
     validator:one_of(afamily, ["ipv4", "ipv6"], _),
     validator:validate(fun ("ipv4") -> {value, inet};
                            ("ipv6") -> {value, inet6}
                        end, afamily, _),
     check_for_raw_addr(_)] ++
        case cluster_compat_mode:is_cluster_madhatter() of
            true ->
                [validator:required(clusterEncryption, _),
                 validator:one_of(clusterEncryption, ["on", "off"], _),
                 validator:validate(fun ("on") -> {value, true};
                                        ("off") -> {value, false}
                                    end, clusterEncryption, _)];
            false ->
                []
        end ++ [check_if_net_config_allowed(_),
                validator:unsupported(_)].

update_type(AFamily, CEncrypt) ->
    CurAFamily = cb_dist:address_family(),
    CurCEncrypt = cb_dist:external_encryption(),
    case {CurAFamily =/= AFamily, CurCEncrypt =/= CEncrypt} of
        {true, _} -> all;
        {false, true} -> external_only;
        {false, false} -> empty
    end.

update_proto_in_dist_config(AFamily, CEncryption) ->
    Listeners = cb_dist:external_listeners(),
    case dist_manager:update_dist_config(Listeners, AFamily, CEncryption) of
        ok ->
            case cb_dist:reload_config() of
                ok -> ok;
                {error, Error} ->
                    erlang:error({reload_cb_dist_config_error, node(), Error})
            end;
        {error, Error} ->
            erlang:error({save_cb_dist_file_error, Error})
    end.

handle_setup_net_config(Req) ->
    menelaus_util:assert_is_enterprise(),
    validator:handle(
      fun (Values) ->
              AFamily = proplists:get_value(afamily, Values),
              CEncrypt = proplists:get_value(clusterEncryption, Values, false),
              erlang:process_flag(trap_exit, true),
              case apply_net_config(AFamily, CEncrypt) of
                  ok -> menelaus_util:reply(Req, 200);
                  {error, Msg} -> menelaus_util:reply_global_error(Req, Msg)
              end,
              erlang:exit(normal)
      end, Req, form, net_config_validators()).

apply_net_config(AFamily, CEncrypt) ->
    ?log_info("Node is going to apply the following net settings: afamily ~p, "
              "encryptiion ~p", [AFamily, CEncrypt]),
    case update_type(AFamily, CEncrypt) of
        empty -> ok;
        Type ->
            try
                update_proto_in_dist_config(AFamily, CEncrypt),
                case Type of
                    external_only -> ok;
                    _ -> change_local_dist_proto(AFamily, false)
                end,
                change_ext_dist_proto(AFamily, CEncrypt),
                ns_config:set({node, node(), address_family},
                              AFamily),
                ns_config:set({node, node(), cluster_encryption},
                              CEncrypt),
                ?log_info("Node network settings (afamily: ~p, encryptiion: ~p)"
                          " successfully applied", [AFamily, CEncrypt]),
                ok
            catch
                error:Error ->
                    Msg = iolist_to_binary(format_error(Error)),
                    ?log_error("~s", [Msg]),
                    {error, Msg}
            end
    end.

change_local_dist_proto(ExpectedFamily, ExpectedEncryption) ->
    ?log_info("Reconnecting to babysitter and restarting couchdb since local "
              "dist protocol settings changed, expected afamily is ~p, "
              "expected encryption is ~p",
              [ExpectedFamily, ExpectedEncryption]),
    Babysitter = ns_server:get_babysitter_node(),
    case rpc:call(Babysitter, cb_dist, reload_config, []) of
        ok -> ok;
        {error, Error} ->
            erlang:error({reload_cb_dist_config_error, Babysitter, Error})
    end,
    ensure_connection_proto(Babysitter,
                            ExpectedFamily, ExpectedEncryption, 10),
    %% Curently couchdb doesn't support gracefull change of afamily
    %% so we have to restart it. Unfortunatelly we can't do it without
    %% restarting ns_server.
    case ns_server_cluster_sup:restart_ns_server() of
        {ok, _} ->
            check_connection_proto(ns_node_disco:couchdb_node(),
                                   ExpectedFamily, ExpectedEncryption);
        {error, not_running} -> ok;
        Error2 -> erlang:error({ns_server_restart_error, Error2})
    end.

change_ext_dist_proto(ExpectedFamily, ExpectedEncryption) ->
    Nodes = ns_node_disco:nodes_wanted() -- [node()],
    ?log_info("Reconnecting to all known erl nodes since dist protocol "
              "settings changed, expected afamily is ~p, expected encryption "
              "is ~p, nodes: ~p", [ExpectedFamily, ExpectedEncryption, Nodes]),
    [ensure_connection_proto(N, ExpectedFamily, ExpectedEncryption, 10)
        || N <- Nodes],
    ok.

ensure_connection_proto(Node, _Family, _Encr, Retries) when Retries =< 0 ->
    erlang:error({exceeded_retries, Node});
ensure_connection_proto(Node, Family, Encryption, Retries) ->
    erlang:disconnect_node(Node),
    case net_kernel:connect(Node) of
        true ->
            ?log_debug("Reconnected to ~p...", [Node]),
            try check_connection_proto(Node, Family, Encryption) of
                ok -> ok
            catch
                error:Reason ->
                    ?log_error("Failed to establish connection to node ~p with "
                               "reason: ~p, retries left: ~p",
                               [Node, Reason, Retries - 1]),
                    timer:sleep(rand:uniform(30)),
                    ensure_connection_proto(Node, Family, Encryption,
                                            Retries - 1)
            end;
        false ->
            ?log_error("Failed to connect to node ~p", [Node]),
            erlang:error({connect_failed, Node})
    end.

check_connection_proto(Node, Family, Encryption) ->
    Proto = case Encryption of
                true -> proxy;
                false -> tcp
            end,
    case net_kernel:node_info(Node) of
        {ok, Info} ->
            case proplists:get_value(address, Info) of
                #net_address{protocol = Proto, family = Family} -> ok;
                %% Workaround for a bug in inet_tls_dist.erl
                %% address family is always set to inet, even when the socket
                %% is actually an inet6 socket
                #net_address{address = {{_, _, _, _, _, _, _, _}, _},
                             protocol = proxy,
                             family = inet} when Proto == proxy,
                                                 Family == inet6 -> ok;
                A -> erlang:error({wrong_proto, Node, A})
            end;
        {error, Error} ->
            erlang:error({node_info, Node, Error})
    end.

format_error({save_cb_dist_file_error, R}) ->
    io_lib:format("Failed to save cb_dist configuration file: ~p", [R]);
format_error({reload_cb_dist_config_error, Node, R}) ->
    io_lib:format("Failed to reload cb_dist configuration file on node ~p: ~p",
                  [Node, R]);
format_error({ns_server_restart_error, Error}) ->
    io_lib:format("Restart error: ~p", [Error]);
format_error({node_info, Node, Error}) ->
    io_lib:format("Failed to get connection info to node ~p: ~p",
                  [Node, Error]);
format_error({wrong_proto, Node, _}) ->
    io_lib:format("Couldn't establish connection of desired type to node ~p",
                  [Node]);
format_error({connect_failed, Node}) ->
    io_lib:format("Couldn't establish connection to node ~p", [Node]);
format_error({exceeded_retries, Node}) ->
    io_lib:format("Reconnect to ~p retries exceeded", [Node]);
format_error(R) ->
    io_lib:format("~p", [R]).

handle_dist_protocols(Req) ->
    validator:handle(
      fun (Props) ->
              handle_dist_protocols_validated(Req, Props)
      end, Req, form, [validator:required(external, _),
                       validate_ext_protos(external, _),
                       validator:unsupported(_)]).

handle_dist_protocols_validated(Req, Props) ->
    Protos = proplists:get_value(external, Props),
    case apply_ext_dist_protocols(Protos) of
        ok -> menelaus_util:reply(Req, 200);
        {error, Msg} -> menelaus_util:reply_global_error(Req, Msg)
    end.

apply_ext_dist_protocols(Protos) ->
    ?log_info("Node is going to change dist protocols to ~p", [Protos]),
    AFamily = cb_dist:address_family(),
    CEncryption = cb_dist:external_encryption(),
    case dist_manager:update_dist_config(Protos, AFamily, CEncryption) of
        ok ->
            case cb_dist:reload_config() of
                ok ->
                    ns_config:set({node, node(), erl_external_dist_protocols},
                                  Protos),
                    ok;
                {error, Error} ->
                    Msg = io_lib:format("Failed to reload cb_dist config: ~p",
                                        [Error]),
                    {error, iolist_to_binary(Msg)}
            end;
        {error, Reason} ->
            Msg = io_lib:format("Failed to store cb_dist config: ~p", [Reason]),
            {error, iolist_to_binary(Msg)}
    end.

validate_ext_protos(Name, State) ->
    validator:validate(
        fun (Value) ->
                Protos = [string:trim(T) || T <- string:tokens(Value, ",")],
                Allowed = ["inet_tcp", "inet6_tcp", "inet_tls", "inet6_tls"],
                case lists:all(lists:member(_, Allowed), Protos) of
                    true ->
                        Res = [list_to_atom(P ++ "_dist") || P <- Protos],
                        case lists:member(inet6_tls_dist, Res) andalso
                             lists:member(inet_tls_dist, Res) of
                            false -> {value, Res};
                            true -> {error, "tls over ipv4 and tls over ipv6 "
                                            "can't be used simultaneously"}
                        end;
                    false ->
                        {error, io_lib:format("invalid protocols list, allowed "
                                              "protocols are ~p", [Allowed])}
                end
        end, Name, State).

-ifdef(TEST).
validate_ix_cbas_path_test() ->
    ?assertEqual(false, validate_ix_cbas_path({path, "/abc/def"}, "/abc/def")),
    ?assertEqual(false, validate_ix_cbas_path({path2, "/abc/def"}, "/abc/def")),
    ?assertMatch({true, _}, validate_ix_cbas_path({path2, "/ab/de"}, "/ab")),
    ?assertMatch({true, _}, validate_ix_cbas_path({path2, "/ab/de/f"}, "/ab")),
    ?assertEqual(false, validate_ix_cbas_path({path2, "/abc/def"}, "/abc/de")),
    ?assertEqual(false, validate_ix_cbas_path({path2, "/abc"}, "/abc/hi")).
-endif.

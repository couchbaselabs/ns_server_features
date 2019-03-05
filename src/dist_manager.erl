%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2018 Couchbase, Inc.
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
%% Distributed erlang configuration and management
%%
-module(dist_manager).

-behaviour(gen_server).

-include("ns_common.hrl").

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([adjust_my_address/3, read_address_config/0, save_address_config/1,
         ip_config_path/0, using_user_supplied_address/0, reset_address/0,
         wait_for_node/1, dist_config_path/1, update_dist_config/3,
         generate_ssl_dist_optfile/0]).

%% custom x509-path validation function used by Erlang TLS distribution.
-export([verify_cert_for_dist/3]).

%% used by babysitter and ns_couchdb
-export([configure_net_kernel/0]).

-record(state, {self_started,
                user_supplied,
                my_ip}).

-define(WAIT_FOR_ADDRESS_ATTEMPTS, 10).
-define(WAIT_FOR_ADDRESS_SLEEP, 1000).

start_link() ->
    proc_lib:start_link(?MODULE, init, [[]]).

ip_config_path() ->
    path_config:component_path(data, "ip").

ip_start_config_path() ->
    path_config:component_path(data, "ip_start").

dist_config_path(DataDir) ->
    filename:join([DataDir, "config", "dist_cfg"]).

using_user_supplied_address() ->
    gen_server:call(?MODULE, using_user_supplied_address).

reset_address() ->
    gen_server:call(?MODULE, reset_address).

store_dist_config(DCfgFile, DCfg) ->
    Data = io_lib:format("~p.~n", [DCfg]),
    misc:atomic_write_file(DCfgFile, Data).

update_dist_config(Listeners, NewAFamily, NewCEncryption) ->
    PreferredExternal =
        case {NewAFamily, NewCEncryption} of
            {inet, false} -> inet_tcp_dist;
            {inet, true} -> inet_tls_dist;
            {inet6, false} -> inet6_tcp_dist;
            {inet6, true} -> inet6_tls_dist
        end,
    PreferredLocal =
        case NewAFamily of
            inet -> inet_tcp_dist;
            inet6 -> inet6_tcp_dist
        end,
    Cfg = [{external_listerners, Listeners} || Listeners =/= undefined] ++
          [{preferred_external_proto, PreferredExternal},
           {preferred_local_proto, PreferredLocal}],
    CfgFile = cb_dist:config_path(),
    case store_dist_config(CfgFile, Cfg) of
        ok ->
            ?log_info("Updated cb_dist config ~p: ~p", [CfgFile, Cfg]),
            ok;
        {error, Reason} ->
            ?log_error("Failed to save cb_dist config to ~p with reason: ~p",
                       [CfgFile, Reason]),
            {error, Reason}
    end.

generate_ssl_dist_optfile() ->
    FilePath = filename:join([path_config:component_path(data),
                              "config", "ssl_dist_opts"]),

    case filelib:is_file(FilePath) of
        true ->
            ok;
        false ->
            CertKeyFile = ns_ssl_services_setup:ssl_cert_key_path(),
            CACertFile = ns_ssl_services_setup:raw_ssl_cacert_key_path(),

            SSLOpts =
                io_lib:format(
                  "\t\t{certfile, ~p},~n"
                  "\t\t{keyfile, ~p},~n"
                  "\t\t{cacertfile, ~p},~n"
                  "\t\t{verify, verify_peer}",
                  [CertKeyFile, CertKeyFile, CACertFile]),

            ServerSSLOpts =
                io_lib:format("\t\t{fail_if_no_peer_cert, true},~n~s", [SSLOpts]),

            ClientSSLOpts =
                io_lib:format("\t\t{verify_fun, "
                              "{fun dist_manager:verify_cert_for_dist/3, []}},~n~s",
                              [SSLOpts]),

            Data =
                io_lib:format(
                  "[~n\t{server, [~n~s~n\t]},~n\t{client, [~n~s~n\t]}~n].",
                  [ServerSSLOpts, ClientSSLOpts]),

            filelib:ensure_dir(FilePath),
            misc:atomic_write_file(FilePath, Data)
    end.

verify_cert_for_dist(_Cert, valid, State) ->
    {valid, State};
verify_cert_for_dist(_Cert, valid_peer, State) ->
    {valid, State};
verify_cert_for_dist(_Cert, {extension, _}, State) ->
    {unknown, State};
verify_cert_for_dist(_Cert, _Event, State) ->
    {fail, State}.

strip_full(String) ->
    String2 = string:strip(String),
    String3 = string:strip(String2, both, $\n),
    String4 = string:strip(String3, both, $\r),
    case String4 =:= String of
        true ->
            String4;
        _ ->
            strip_full(String4)
    end.

read_address_config() ->
    IpStartPath = ip_start_config_path(),
    case read_address_config_from_path(IpStartPath) of
        Address when is_list(Address) ->
            {Address, true};
        read_error ->
            read_error;
        undefined ->
            IpPath = ip_config_path(),
            case read_address_config_from_path(IpPath) of
                Address when is_list(Address) ->
                    {Address, false};
                Other ->
                    Other
            end
    end.

read_address_config_from_path(Path) ->
    ?log_info("Reading ip config from ~p", [Path]),
    case read_from_file(Path) of
        {error, Error} ->
            ?log_error("Failed to read ip config from `~s`: ~p",
                       [Path, Error]);
        RV ->
            RV
    end.

read_from_file(Path) ->
    case file:read_file(Path) of
        {ok, BinaryContents} ->
            case strip_full(binary_to_list(BinaryContents)) of
                "" ->
                    undefined;
                V ->
                    V
            end;
        {error, enoent} ->
            undefined;
        Error ->
            Error
    end.

wait_for_address(Address) ->
    wait_for_address(Address, ?WAIT_FOR_ADDRESS_ATTEMPTS).

wait_for_address(_Address, 0) ->
    bad_address;
wait_for_address(Address, N) ->
    case misc:is_good_address(Address) of
        ok ->
            ok;
        {address_not_allowed, Message}  ->
            ?log_error("Desired address ~s is not allowed by erlang: ~s", [Address, Message]),
            bad_address;
        Other ->
            case Other of
                {cannot_resolve, Errno} ->
                    ?log_warning("Could not resolve address `~s`: ~p",
                                 [Address, Errno]);
                {cannot_listen, Errno} ->
                    ?log_warning("Cannot listen on address `~s`: ~p",
                                 [Address, Errno])
            end,

            ?log_info("Configured address `~s` seems to be invalid. "
                      "Giving OS a chance to bring it up.", [Address]),
            timer:sleep(?WAIT_FOR_ADDRESS_SLEEP),
            wait_for_address(Address, N - 1)
    end.

save_address_config(#state{my_ip = MyIP,
                           user_supplied = UserSupplied}) ->
    PathPair = [ip_start_config_path(), ip_config_path()],
    [Path, ClearPath] =
        case UserSupplied of
            true ->
                PathPair;
            false ->
                lists:reverse(PathPair)
        end,
    DeleteRV = file:delete(ClearPath),
    ?log_info("Deleting irrelevant ip file ~p: ~p", [ClearPath, DeleteRV]),
    ?log_info("saving ip config to ~p", [Path]),
    misc:atomic_write_file(Path, MyIP).

save_node(NodeName, Path) ->
    ?log_info("saving node to ~p", [Path]),
    misc:atomic_write_file(Path, NodeName ++ "\n").

save_node(NodeName) ->
    case application:get_env(nodefile) of
        {ok, undefined} -> nothing;
        {ok, NodeFile} -> save_node(NodeName, NodeFile);
        X -> X
    end.

init([]) ->
    register(?MODULE, self()),
    net_kernel:stop(),

    {Address, UserSupplied} =
        case read_address_config() of
            undefined ->
                ?log_info("ip config not found. Looks like we're brand new node"),
                {misc:localhost(), false};
            read_error ->
                ?log_error("Could not read ip config. "
                           "Will refuse to start for safety reasons."),
                ale:sync(?NS_SERVER_LOGGER),
                misc:halt(1);
            V ->
                V
        end,

    case wait_for_address(Address) of
        ok ->
            ok;
        bad_address ->
            ?log_error("Configured address `~s` seems to be invalid. "
                       "Will refuse to start for safety reasons.", [Address]),
            ale:sync(?NS_SERVER_LOGGER),
            misc:halt(1)
    end,

    State = bringup(Address, UserSupplied),
    proc_lib:init_ack({ok, self()}),
    case misc:read_marker(ns_cluster:rename_marker_path()) of
        {ok, OldNode} ->
            ?log_debug("Found rename marker. Old Node = ~p", [OldNode]),
            complete_rename(list_to_atom(OldNode));
        _ ->
            ok
    end,

    misc:wait_for_local_name(ns_config, 60000),
    Self = self(),
    EventHandler =
        fun ({erl_external_dist_protocols, _} = E) -> Self ! E;
            (_) -> ok
        end,
    ns_pubsub:subscribe_link(ns_config_events, EventHandler),

    gen_server:enter_loop(?MODULE, [], State).

%% There are only two valid cases here:
%% 1. Successfully started
decode_status({ok, _Pid}) ->
    true;
%% 2. Already initialized (via -name or -sname)
decode_status({error, {{already_started, _Pid}, _Stack}}) ->
    false.

-spec adjust_my_address(string(), boolean(), fun()) ->
                               net_restarted | not_self_started | nothing |
                               {address_save_failed, term()}.
adjust_my_address(MyIP, UserSupplied, OnRename) ->
    gen_server:call(?MODULE, {adjust_my_address, MyIP, UserSupplied, OnRename}, infinity).

%% Bring up distributed erlang.
bringup(MyIP, UserSupplied) ->
    ShortName = misc:get_env_default(short_name, "ns_1"),
    MyNodeNameStr = ShortName ++ "@" ++ MyIP,
    MyNodeName = list_to_atom(MyNodeNameStr),

    ?log_info("Attempting to bring up net_kernel with name ~p", [MyNodeName]),
    ok = misc:wait_for_nodename(ShortName),
    Rv = decode_status(net_kernel:start([MyNodeName, longnames])),
    net_kernel:set_net_ticktime(misc:get_env_default(set_net_ticktime, 60)),

    ok = configure_net_kernel(),
    ns_server:setup_node_names(),

    erlang:set_cookie(ns_node_disco:couchdb_node(), ns_server:get_babysitter_cookie()),

    %% Rv can be false in case -name has been passed to erl but we still need
    %% to save the node name to be able to shutdown the server gracefully.
    ActualNodeName = erlang:atom_to_list(node()),
    RN = save_node(ActualNodeName),
    ?log_debug("Attempted to save node name to disk: ~p", [RN]),

    wait_for_node(ns_server:get_babysitter_node()),

    #state{self_started = Rv, my_ip = MyIP, user_supplied = UserSupplied}.

wait_for_node(Node) when is_atom(Node) ->
    ?log_debug("Waiting for connection to node ~p to be established", [Node]),
    wait_for_node(fun () -> Node end);
wait_for_node(NodeFun) ->
    wait_for_node(NodeFun, 100, 10).

wait_for_node(NodeFun, _Time, 0) ->
    ?log_error("Failed to wait for node ~p", [NodeFun()]),
    erlang:exit({error, wait_for_node_failed});
wait_for_node(NodeFun, Time, Try) ->
    Node = NodeFun(),
    case net_kernel:connect_node(Node) of
        true ->
            ?log_debug("Observed node ~p to come up", [Node]),
            ok;
        Ret ->
            ?log_debug("Node ~p is not accessible yet. (Ret = ~p). Retry in ~p ms.", [Node, Ret, Time]),
            timer:sleep(Time),
            wait_for_node(NodeFun, Time, Try - 1)
    end.

configure_net_kernel() ->
    Verbosity = misc:get_env_default(ns_server, net_kernel_verbosity, 0),
    RV = net_kernel:verbose(Verbosity),
    ?log_debug("Set net_kernel vebosity to ~p -> ~p", [Verbosity, RV]),
    ok.

%% Tear down distributed erlang.
teardown() ->
    misc:executing_on_new_process(
      fun () ->
              Node = node(),
              ok = net_kernel:monitor_nodes(true, [nodedown_reason]),
              ok = net_kernel:stop(),

              receive
                  {nodedown, DownNode, _Info} = Msg when DownNode =:= Node ->
                      ?log_debug("Got nodedown msg ~p after terminating net kernel",
                                 [Msg]),
                      ok
              end
      end).

do_adjust_address(MyIP, UserSupplied, OnRename, State = #state{my_ip = MyOldIP}) ->
    OldNode = node(),
    {NewState, Status} =
        case MyOldIP of
            MyIP ->
                {State#state{user_supplied = UserSupplied}, nothing};
            _ ->
                OnRename(),
                Cookie = erlang:get_cookie(),
                teardown(),
                ?log_info("Adjusted IP to ~p", [MyIP]),
                NewState1 = bringup(MyIP, UserSupplied),
                if
                    NewState1#state.self_started ->
                        ?log_info("Re-setting cookie ~p",
                                  [{ns_cookie_manager:sanitize_cookie(Cookie), node()}]),
                        erlang:set_cookie(node(), Cookie);
                    true -> ok
                end,
                misc:create_marker(ns_cluster:rename_marker_path(), atom_to_list(OldNode)),
                {NewState1, net_restarted}
        end,

    case save_address_config(NewState) of
        ok ->
            ?log_info("Persisted the address successfully"),
            case Status of
                net_restarted ->
                    master_activity_events:note_name_changed(),
                    complete_rename(OldNode);
                _ ->
                    ok
            end,
            {reply, Status, NewState};
        {error, Error} ->
            ?log_warning("Failed to persist the address: ~p", [Error]),
            {stop,
             {address_save_failed, Error},
             {address_save_failed, Error},
             State}
    end.

notify_couchdb_node(NewNSServerNodeName) ->
    %% is_couchdb_node_started is raceful, but if node starts right after is_couchdb_node_started
    %% and before we try to update, that means that it already started with the correct ns_server
    %% node name as a parameter
    case ns_server_nodes_sup:is_couchdb_node_started() of
        true ->
            wait_for_node(ns_node_disco:couchdb_node()),
            ok = ns_couchdb_config_rep:update_ns_server_node_name(NewNSServerNodeName);
        false ->
            ?log_debug("Couchdb node is not started. Don't need to notify")
    end.

complete_rename(OldNode) ->
    NewNode = node(),
    case OldNode of
        NewNode ->
            ?log_debug("Rename marker exists but node name didn't change. Nothing to do.");
        _ ->
            notify_couchdb_node(NewNode),
            ?log_debug("Renaming node from ~p to ~p.", [OldNode, NewNode]),
            rename_node_in_config(OldNode, NewNode),
            ?log_debug("Node ~p has been renamed to ~p.", [OldNode, NewNode])
    end,
    misc:remove_marker(ns_cluster:rename_marker_path()).

rename_node_in_config(Old, New) ->
    misc:wait_for_local_name(ns_config, 60000),
    misc:wait_for_local_name(ns_config_rep, 60000),
    ns_config:update(fun ({K, V}) ->
                             NewK = misc:rewrite_value(Old, New, K),
                             NewV = misc:rewrite_value(Old, New, V),
                             if
                                 NewK =/= K orelse NewV =/= V ->
                                     ?log_debug("renaming node conf ~p -> ~p:~n  ~p ->~n  ~p",
                                                [K, NewK, ns_config_log:sanitize(V),
                                                 ns_config_log:sanitize(NewV)]),
                                     {update, {NewK, NewV}};
                                 true ->
                                     skip
                             end
                     end),
    ns_config_rep:ensure_config_seen_by_nodes().

handle_call({adjust_my_address, _, _, _}, _From,
            #state{self_started = false} = State) ->
    {reply, not_self_started, State};
handle_call({adjust_my_address, MyIP, true, OnRename}, From, State) ->
    case misc:is_localhost(MyIP) of
        true ->
            handle_call({adjust_my_address, MyIP, false, OnRename}, From, State);
        false ->
            do_adjust_address(MyIP, true, OnRename, State)
    end;
handle_call({adjust_my_address, _MyIP, false = _UserSupplied, _}, _From,
            #state{user_supplied = true} = State) ->
    {reply, nothing, State};
handle_call({adjust_my_address, MyOldIP, UserSupplied, _}, _From,
            #state{my_ip = MyOldIP, user_supplied = UserSupplied} = State) ->
    {reply, nothing, State};
handle_call({adjust_my_address, MyIP, UserSupplied, OnRename}, _From,
            State) ->
    do_adjust_address(MyIP, UserSupplied, OnRename, State);

handle_call(using_user_supplied_address, _From,
            #state{user_supplied = UserSupplied} = State) ->
    {reply, UserSupplied, State};
handle_call(reset_address, _From,
            #state{self_started = true,
                   user_supplied = true} = State) ->
    ?log_info("Going to mark current user-supplied address as non-user-supplied address"),
    NewState = State#state{user_supplied = false},
    case save_address_config(NewState) of
        ok ->
            ?log_info("Persisted the address successfully"),
            {reply, ok, NewState};
        {error, Error} ->
            ?log_warning("Failed to persist the address: ~p", [Error]),
            {stop,
             {address_save_failed, Error},
             {address_save_failed, Error},
             State}
    end;
handle_call(reset_address, _From, State) ->
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, unhandled, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({erl_external_dist_protocols, _}, State) ->
    update_dist_protocols(),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_dist_protocols() ->
    Listeners = ns_config:read_key_fast(erl_external_dist_protocols, undefined),
    AFamily = ns_config:search_node_with_default(address_family, inet),
    CEncryption = ns_config:search_node_with_default(cluster_encryption, false),
    case update_dist_config(Listeners, AFamily, CEncryption) of
        ok ->
            case cb_dist:reload_config() of
                ok -> ok;
                {error, Error} ->
                    ?log_error("Failed to reload cb_dist config: ~p", [Error]),
                    {error, Error}
            end;
        {error, Reason} -> {error, Reason}
    end.

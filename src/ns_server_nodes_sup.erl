%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2019 Couchbase, Inc.
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
%% @doc supervisor that ensures that the whole ns_server_sup will be restarted
%%      if couchdb_node port crashes
%%

-module(ns_server_nodes_sup).

-include("ns_common.hrl").

-behaviour(supervisor2).

%% API
-export([start_link/0, start_couchdb_node/0, is_couchdb_node_started/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {rest_for_one,
            misc:get_env_default(max_r, 3),
            misc:get_env_default(max_t, 10)}, child_specs()} }.

child_specs() ->
    [
     {remote_monitors, {remote_monitors, start_link, []},
      permanent, 1000, worker, []},

     %% needs to be started before ns_ssl_services_sup because ssl endpoint is
     %% started there
     menelaus_sup:barrier_spec(menelaus_barrier),

     %% connection pool that we use when talking to goxdcr/indexer/cbq-engine
     {rest_lhttpc_pool, {lhttpc_manager, start_link,
                         [[{name, rest_lhttpc_pool},
                           {connection_timeout, 120000},
                           {pool_size, 20}]]},
      {permanent, 1}, 1000, worker, [lhttpc_manager]},

     {memcached_refresh, {memcached_refresh, start_link, []},
      permanent, 1000, worker, []},

     {ns_ssl_services_sup,
      {ns_ssl_services_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {ldap_auth_cache, {ldap_auth_cache, start_link, []},
      permanent, 1000, worker, []},

     {users_sup,
      {users_sup, start_link, []},
      permanent, infinity, supervisor, []},

     %% we cannot "kill" this guy anyways. Thus hefty shutdown timeout.
     {start_couchdb_node, {?MODULE, start_couchdb_node, []},
      {permanent, 5}, 86400000, worker, []},

     {wait_for_couchdb_node, {erlang, apply, [fun wait_link_to_couchdb_node/0, []]},
      permanent, 1000, worker, []},

     {setup_dirs,
      {ns_storage_conf, setup_db_and_ix_paths, []},
      transient, brutal_kill, worker, []},

     {ns_server_sup, {ns_server_sup, start_link, []},
      permanent, infinity, supervisor, [ns_server_sup]},

     menelaus_sup:barrier_notify_spec(menelaus_barrier_notify)].

create_ns_couchdb_spec() ->
    CouchIni = case init:get_argument(couch_ini) of
                   error ->
                       [];
                   {ok, [[]]} ->
                       [];
                   {ok, [Values]} ->
                       ["-couch_ini" | Values]
               end,

    %% These are always passed down from the babysitter during startup.
    %% Passing it down to couchdb VM to restrict its listening port range.
    {ok, ListenMin} = application:get_env(kernel, inet_dist_listen_min),
    {ok, ListenMax} = application:get_env(kernel, inet_dist_listen_max),

    ErlangArgs = CouchIni ++
        ["-setcookie", atom_to_list(ns_server:get_babysitter_cookie()),
         "-name", atom_to_list(ns_node_disco:couchdb_node()),
         "-smp", "enable",
         "+P", "327680",
         "+K", "true",
         "-kernel", "error_logger", "false",
         "inet_dist_listen_min", integer_to_list(ListenMin),
         "inet_dist_listen_max", integer_to_list(ListenMax),
         "-sasl", "sasl_error_logger", "false",
         "-nouser",
         "-hidden",
         "-proto_dist", "cb",
         "-epmd_module", atom_to_list(net_kernel:epmd_module()),
         "-run", "child_erlang", "child_start", "ns_couchdb"],

    ns_ports_setup:create_erl_node_spec(
      ns_couchdb, [{ns_server_node, node()}], "NS_COUCHDB_ENV_ARGS", ErlangArgs).

start_couchdb_node() ->
    ns_port_server:start_link_named(
      ns_couchdb_port,
      fun () ->
              ShortName = misc:node_name_short(ns_node_disco:couchdb_node()),
              ok = misc:wait_for_nodename(ShortName),
              create_ns_couchdb_spec()
      end).

wait_link_to_couchdb_node() ->
    proc_lib:start_link(erlang, apply, [fun start_wait_link_to_couchdb_node/0, []]).

start_wait_link_to_couchdb_node() ->
    erlang:register(wait_link_to_couchdb_node, self()),
    do_wait_link_to_couchdb_node(true).

do_wait_link_to_couchdb_node(Initial) ->
    ?log_debug("Waiting for ns_couchdb node to start"),
    Self = self(),
    Ref = make_ref(),
    MRef = erlang:monitor(process, erlang:whereis(ns_couchdb_port)),

    proc_lib:spawn_link(
      fun () ->
              RV = misc:poll_for_condition(
                     fun () ->
                             case rpc:call(ns_node_disco:couchdb_node(),
                                           erlang, apply,
                                           [fun is_couchdb_node_ready/0, []], 5000) of
                                 {ok, _} = OK -> OK;
                                 Other ->
                                     ?log_debug("ns_couchdb is not ready: ~p", [Other]),
                                     false
                             end
                     end,
                     60000, 200),
              Self ! {Ref, RV}
      end),

    receive
        {Ref, RV} ->
            case RV of
                {ok, Pid} ->
                    case Initial of
                        true ->
                            proc_lib:init_ack({ok, self()});
                        _ -> ok
                    end,
                    remote_monitors:monitor(Pid),
                    wait_link_to_couchdb_node_loop(Pid);
                timeout ->
                    exit(timeout)
            end;
        {'DOWN', MRef, process, Pid, Reason} ->
            ?log_error("ns_couchdb_port(~p) died with reason ~p", [Pid, Reason]),
            exit(Reason)
    end.

wait_link_to_couchdb_node_loop(Pid) ->
    receive
        {remote_monitor_down, Pid, unpaused} ->
            ?log_debug("Link to couchdb node was unpaused."),
            do_wait_link_to_couchdb_node(false);
        {remote_monitor_down, Pid, Reason} ->
            ?log_debug("Link to couchdb node was lost. Reason: ~p", [Reason]),
            exit(normal);
        Msg ->
            ?log_debug("Exiting due to message: ~p", [Msg]),
            exit(normal)
    end.

%% NOTE: rpc-ed between ns_server and ns_couchdb nodes.
is_couchdb_node_ready() ->
    case erlang:whereis(ns_couchdb_sup) of
        P when is_pid(P) ->
            try supervisor:which_children(P) of
                _ ->
                    {ok, P}
            catch _:_ ->
                    false
            end;
        _ ->
            false
    end.

is_couchdb_node_started() ->
    is_pid(erlang:whereis(ns_couchdb_port)).

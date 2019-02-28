%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2016 Couchbase, Inc.
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

-module(ns_babysitter_sup).

-behavior(supervisor).

-include("ns_common.hrl").

-export([start_link/0,
         reconfig_and_restart_children/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 3, 10},
          child_specs()}}.

child_specs() ->
    [{ns_crash_log, {ns_crash_log, start_link, []},
      permanent, 1000, worker, []}] ++
        case ns_config_default:init_is_enterprise() of
            true ->
                [{encryption_service, {encryption_service, start_link, []},
                  permanent, 1000, worker, []}];
            false ->
                []
        end ++
        [{child_ns_server_sup, {child_ns_server_sup, start_link, []},
          permanent, infinity, supervisor, []},
         {ns_child_ports_sup, {ns_child_ports_sup, start_link, []},
          permanent, infinity, supervisor, []},
         {ns_ports_manager, {ns_ports_manager, start_link, []},
          permanent, 1000, worker, []}].

reconfig_and_restart_children() ->
    ShortName = misc:node_name_short(),
    try
        %% Terminate the ns_server VM, restart the net_kernel on the baybsitter
        %% VM using the new distribution type and restart the ns_server VM.
        ok = supervisor:terminate_child(ns_babysitter_sup, child_ns_server_sup),
        ok = net_kernel:stop(),
        ok = ns_babysitter:setup_env_and_dist_from_config(ShortName, "start"),
        {ok, _} = supervisor:restart_child(ns_babysitter_sup,
                                           child_ns_server_sup)
    catch
        _T:Error ->
            ale:info(?USER_LOGGER, "Attempt to restart Couchbase server "
                     "automatically failed. Need a manual restart for the "
                     "new config to take effect (Error: ~p)", Error)
    end.

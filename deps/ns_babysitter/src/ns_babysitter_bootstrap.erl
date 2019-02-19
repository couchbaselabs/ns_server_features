%% @author Couchbase <info@couchbase.com>
%% @copyright 2013-2018 Couchbase, Inc.
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
-module(ns_babysitter_bootstrap).

-export([start/0,
         stop/0,
         get_quick_stop/0,
         remote_stop/2,
         override_resolver/0]).

-include("ns_common.hrl").

start() ->
    try
        ok = application:start(ale),
        ok = application:start(sasl),
        ok = application:start(ns_babysitter, permanent),
        (catch ?log_info("~s: babysitter has started", [os:getpid()])),
        ns_babysitter:make_pidfile()
    catch T:E ->
            timer:sleep(500),
            erlang:T(E)
    end.

stop() ->
    (catch ?log_info("~s: got shutdown request. Terminating.", [os:getpid()])),
    application:stop(ns_babysitter),
    ale:sync_all_sinks(),
    ns_babysitter:delete_pidfile(),
    init:stop().

remote_stop(Node, DCfgFile) ->
    %% Make sure that the node is not named as we will rename the node
    %% based on the distribution config stored in the file.
    'nonode@nohost' = node(),

    %% Start the net_kernel in the distribution mode as defined in the
    %% config file so that this VM can talk to the babysitter VM. Then
    %% invoke the 'stop' API on the babysitter node via an RPC.
    ns_babysitter:start_erl_distribution(DCfgFile, "executioner", "stop"),

    RV = rpc:call(Node, ns_babysitter_bootstrap, stop, []),
    ExitStatus = case RV of
                     ok -> 0;
                     Other ->
                         io:format("NOTE: shutdown failed~n~p~n", [Other]),
                         1
                 end,
    init:stop(ExitStatus).

get_quick_stop() ->
    fun quick_stop/0.

quick_stop() ->
    application:set_env(ns_babysitter, port_shutdown_command, "die!"),
    stop().


override_resolver() ->
    inet_db:set_lookup([file, dns]),
    start().

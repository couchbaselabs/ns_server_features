%% @author Couchbase <info@couchbase.com>
%% @copyright 2012-2018 Couchbase, Inc.
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
-module(samples_loader_tasks).

-behaviour(gen_server).

-include("ns_common.hrl").

%% gen_server API
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_loading_sample/2, get_tasks/1]).

-export([perform_loading_task/2]).

start_loading_sample(Name, Quota) ->
    gen_server:call(?MODULE, {start_loading_sample, Name, Quota}, infinity).

get_tasks(Timeout) ->
    gen_server:call(?MODULE, get_tasks, Timeout).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-record(state, {
          tasks = [] :: [{string(), pid()}],
          token_pid :: undefined | pid()
         }).

init([]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({start_loading_sample, Name, Quota}, _From, #state{tasks = Tasks} = State) ->
    case lists:keyfind(Name, 1, Tasks) of
        false ->
            Pid = start_new_loading_task(Name, Quota),
            ns_heart:force_beat(),
            NewState = State#state{tasks = [{Name, Pid} | Tasks]},
            {reply, ok, maybe_pass_token(NewState)};
        _ ->
            {reply, already_started, State}
    end;
handle_call(get_tasks, _From, State) ->
    {reply, State#state.tasks, State}.


handle_cast(_, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason} = Msg, #state{tasks = Tasks,
                                                token_pid = TokenPid} = State) ->
    case lists:keyfind(Pid, 2, Tasks) of
        false ->
            ?log_error("Got exit not from child: ~p", [Msg]),
            exit(Reason);
        {Name, _} ->
            ?log_debug("Consumed exit signal from samples loading task ~s: ~p", [Name, Msg]),
            ns_heart:force_beat(),
            case Reason of
                normal ->
                    ale:info(?USER_LOGGER, "Completed loading sample bucket ~s", [Name]);
                {failed_to_load_samples, Status, Output} ->
                    ale:error(?USER_LOGGER,
                              "Loading sample bucket ~s failed. "
                              "Samples loader exited with status ~b.~n"
                              "Loader's output was:~n~n~s",
                              [Name, Status, Output]);
                _ ->
                    ale:error(?USER_LOGGER,
                              "Loading sample bucket ~s failed: ~p",
                              [Name, Reason])
            end,
            NewTokenPid = case Pid =:= TokenPid of
                              true ->
                                  ?log_debug("Token holder died"),
                                  undefined;
                              _ ->
                                  TokenPid
                          end,
            NewState = State#state{tasks = lists:keydelete(Pid, 2, Tasks),
                                   token_pid = NewTokenPid},
            {noreply, maybe_pass_token(NewState)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_pass_token(#state{token_pid = undefined,
                        tasks = [{Name, FirstPid}|_]} = State) ->
    FirstPid ! allowed_to_go,
    ?log_info("Passed samples loading token to task: ~s", [Name]),
    State#state{token_pid = FirstPid};
maybe_pass_token(State) ->
    State.

start_new_loading_task(Name, Quota) ->
    proc_lib:spawn_link(?MODULE, perform_loading_task, [Name, Quota]).

perform_loading_task(Name, Quota) ->
    receive
        allowed_to_go -> ok
    end,

    Host = misc:extract_node_address(node()),
    Port = service_ports:get_port(rest_port),
    BinDir = path_config:component_path(bin),

    Cmd = BinDir ++ "/cbdocloader",
    Args = ["--cluster", misc:join_host_port(Host, Port),
            "--bucket", Name,
            "--bucket-quota", integer_to_list(Quota),
            "--threads", "2",
            "--verbose",
            "--dataset", filename:join([BinDir, "..",
                                        "samples", Name ++ ".zip"])],

    Env = [{"CB_USERNAME", "@ns_server"},
           {"CB_PASSWORD", ns_config_auth:get_password(special)}],

    {Status, Output} = misc:run_external_tool(Cmd, Args, Env),
    case Status of
        0 ->
            ok;
        _ ->
            exit({failed_to_load_samples, Status, Output})
    end.

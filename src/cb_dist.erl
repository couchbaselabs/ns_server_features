-module(cb_dist).

-behaviour(gen_server).

-include_lib("kernel/include/net_address.hrl").
-include_lib("kernel/include/dist_util.hrl").

% dist module callbacks, called from net_kernel
-export([listen/1, accept/1, accept_connection/5,
         setup/5, close/1, select/1, is_node_name/1, childspecs/0]).

% management api
-export([start_link/0,
         get_prefered_dist/1,
         set_prefered_protocol/2,
         enable_protocol/1,
         disable_protocol/1,
         status/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(s, {listeners = [],
            acceptors = [],
            creation = undefined,
            kernel_pid = undefined,
            prefered_proto = undefined,
            prefered_local_proto = undefined,
            name = undefined}).

-define(family, ?MODULE).
-define(proto, ?MODULE).

-type socket() :: any().
-type protocol() :: inet_tcp_dist | inet6_tcp_dist |
                    inet_tls_dist | inet6_tls_dist.

%%%===================================================================
%%% API
%%%===================================================================

childspecs() ->
    CBDistSpec = [{?MODULE, {?MODULE, start_link, []},
                   permanent, infinity, worker, [?MODULE]}],
    DistSpecs =
        lists:flatmap(
          fun (Mod) ->
                  case (catch Mod:childspecs()) of
                      {ok, Childspecs} when is_list(Childspecs) -> Childspecs;
                      _ -> []
                  end
          end, [inet_tcp_dist, inet_tls_dist]),
    {ok, CBDistSpec ++ DistSpecs}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec listen(Name :: atom()) ->
    {ok, {LSocket :: any(),
          LocalTcpAddress :: #net_address{},
          Creation :: pos_integer()}}.
listen(Name) when is_atom(Name) ->
    Pid = whereis(?MODULE),
    Creation = gen_server:call(Pid, {listen, Name}, infinity),
    Addr = #net_address{address = undefined,
                        host = undefined,
                        protocol = ?family,
                        family = ?proto},
    {ok, {Pid, Addr, Creation}}.

-spec accept(LSocket :: any()) -> AcceptorPid :: pid().
accept(_LSocket) ->
    gen_server:call(?MODULE, {accept, self()}, infinity).

-spec accept_connection(CBDistPid :: pid(),
                        Acceptor :: {pid(), socket()},
                        MyNode :: atom(),
                        Allowed :: any(),
                        SetupTime :: any()) ->
                            {ConPid :: pid(), AcceptorPid :: pid()}.
accept_connection(_, {AcceptorPid, ConnectionSocket}, MyNode, Allowed, SetupTime) ->
    Module = gen_server:call(?MODULE, {get_module_by_acceptor, AcceptorPid},
                             infinity),
    info_msg("Accepting connection from acceptor ~p using module ~p",
             [AcceptorPid, Module]),
    case Module =/= undefined of
        true ->
            ConPid = Module:accept_connection(AcceptorPid, ConnectionSocket,
                                              MyNode, Allowed, SetupTime),
            {ConPid, AcceptorPid};
        false ->
            {spawn_opt(fun () ->
                          error_msg("** Connection from unknown acceptor ~p, "
                                    "please reconnect ** ~n", [AcceptorPid]),
                          ?shutdown(no_node)
                      end, [link]), AcceptorPid}
    end.

-spec select(Node :: atom()) -> true | false.
select(Node) ->
    Module = get_prefered_dist(Node),
    Module:select(Node).

-spec setup(Node :: atom(),
            Type :: hidden | normal,
            MyNode :: atom(),
            LongOrShortNames :: any(),
            SetupTime :: any()) -> ConPid :: pid().
setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    Module = get_prefered_dist(Node),
    info_msg("Setting up new connection to ~p using ~p", [Node, Module]),
    Module:setup(Node, Type, MyNode, LongOrShortNames, SetupTime).

-spec is_node_name(Node :: atom()) -> true | false.
is_node_name(Node) ->
    Module = get_prefered_dist(Node),
    Module:is_node_name(Node).

-spec close(LSocket :: any()) -> ok.
close(_LSocket) ->
    gen_server:call(?MODULE, close, infinity).

-spec get_prefered_dist(TargetNode :: atom() | string()) -> protocol().
get_prefered_dist(TargetNode) ->
    gen_server:call(?MODULE, {get_prefered, TargetNode}, infinity).

-spec set_prefered_protocol(ComType :: local | external, Proto :: protocol()) ->
        ok | {error, Reason :: any()}.
set_prefered_protocol(ComType, Protocol) ->
    gen_server:call(?MODULE, {set_prefered_protocol, ComType, Protocol}).

-spec enable_protocol(Protocol :: protocol()) -> ok | {error, Reason :: any()}.
enable_protocol(Protocol) ->
    gen_server:call(?MODULE, {enable_protocol, Protocol}).

-spec disable_protocol(Protocol :: protocol()) -> ok | {error, Reason :: any()}.
disable_protocol(Protocol) ->
    gen_server:call(?MODULE, {disable_protocol, Protocol}).

status() ->
    gen_server:call(?MODULE, status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    info_msg("Starting cb_dist with config ~p", [Config]),
    process_flag(trap_exit,true),
    {ok, #s{creation = rand:uniform(4) - 1}}.

handle_call({listen, Name}, _From, #s{creation = Creation} = State) ->
    LocalProto = os:getenv("local_dist_type", "inet_tcp") ++ "_dist",
    GlobalProto =
        case cb_epmd:node_type(atom_to_list(Name)) of
            {ok, ns_server, _} ->
                os:getenv("global_dist_type", "inet_tcp") ++ "_dist";
            {ok, _, _} ->
                LocalProto
        end,
    Protos = lists:usort([list_to_atom(P) || P <- [LocalProto, GlobalProto]]),

    info_msg("Initial protos: ~p", [Protos]),

    Listeners =
        lists:filtermap(
            fun (Module) ->
                    case listen_proto(Module, Name) of
                        {ok, Res} -> {true, {Module, Res}};
                        _Error -> false
                    end
            end, Protos),
    {reply, Creation, State#s{listeners = Listeners,
                              name = Name,
                              prefered_proto = list_to_atom(GlobalProto),
                              prefered_local_proto = list_to_atom(LocalProto)}};

handle_call({accept, KernelPid}, _From, #s{listeners = Listeners} = State) ->
    Acceptors =
        lists:map(
            fun ({Module, {LSocket, _Addr, _Creation}}) ->
                {Module:accept(LSocket), Module}
            end,
            Listeners),
    {reply, self(), State#s{acceptors = Acceptors, kernel_pid = KernelPid}};

handle_call({get_module_by_acceptor, AcceptorPid}, _From,
            #s{acceptors = Acceptors} = State) ->
    Module = proplists:get_value(AcceptorPid, Acceptors),
    {reply, Module, State};

handle_call({get_prefered, Target}, _From,
            #s{prefered_proto = Proto,
               prefered_local_proto = LocalProto,
               name = Name} = State) ->
    IsLocalDest = cb_epmd:is_local_node(Target),
    IsLocalSource = cb_epmd:is_local_node(Name),
    Res =
        case IsLocalDest or IsLocalSource of
            true -> LocalProto;
            false -> Proto
        end,
    {reply, Res, State};

handle_call(close, _From, State) ->
    {stop, normal, ok, close_listeners(State)};

handle_call({set_prefered_protocol, local, P}, _From, State) ->
    case is_valid_protocol(P) of
        true -> {reply, ok, State#s{prefered_local_proto = P}};
        false -> {reply, {error, invalid_protocol}, State}
    end;

handle_call({set_prefered_protocol, external, P}, _From, State) ->
    case is_valid_protocol(P) of
        true -> {reply, ok, State#s{prefered_proto = P}};
        false -> {reply, {error, invalid_protocol}, State}
    end;

handle_call({enable_protocol, P}, _From, State) ->
    case can_add_proto(P, State) of
        ok ->
            {Res, NewState} = add_proto(P, State),
            {reply, Res, NewState};
        {error, Reason} ->
            {reply, {errror, Reason}, State}
    end;

handle_call({disable_protocol, P}, _From, #s{listeners = Listeners} = State) ->
    case proplists:is_defined(P, Listeners) of
        true ->
            NewState = remove_proto(P, State),
            {reply, ok, NewState};
        false ->
            {reply, {error, not_enabled}, State}
    end;

handle_call(status, _From, #s{listeners = Listeners,
                              acceptors = Acceptors,
                              prefered_proto = Prefered,
                              prefered_local_proto = PreferedLocal,
                              name = Name} = State) ->
    {reply, [{name, Name},
             {prefered_proto, Prefered},
             {prefered_local_proto, PreferedLocal},
             {protos, [M || {M, _} <- Listeners]},
             {listeners, Listeners},
             {acceptors, Acceptors}], State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({accept, AcceptorPid, ConSocket, _Family, _Protocol},
            #s{kernel_pid = KernelPid} = State) ->
    info_msg("Accepted new connection from ~p", [AcceptorPid]),
    KernelPid ! {accept, self(), {AcceptorPid, ConSocket}, ?family, ?proto},
    {noreply, State};

handle_info({KernelPid, controller, {ConPid, AcceptorPid}},
            #s{kernel_pid = KernelPid} = State) ->
    AcceptorPid ! {self(), controller, ConPid},
    {noreply, State};

handle_info({'EXIT', Kernel, Reason}, State = #s{kernel_pid = Kernel}) ->
    error_msg("received EXIT from kernel, stoping: ~p", [Reason]),
    {stop, Reason, State};

handle_info({'EXIT', From, Reason}, State) ->
    error_msg("received EXIT from ~p, stoping: ~p", [From, Reason]),
    {stop, {'EXIT', From, Reason}, State};

handle_info(Info, State) ->
    error_msg("received unknown message: ~p", [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    error_msg("terminating with reason: ~p", [Reason]),
    close_listeners(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

close_listeners(#s{listeners = Listeners} = State) ->
    Protos = [M || {M, _} <- Listeners],
    lists:foldl(fun (M, S) -> remove_proto(M, S) end, State, Protos).

with_dist_port(Port, Fun) ->
    OldMin = application:get_env(kernel,inet_dist_listen_min),
    OldMax = application:get_env(kernel,inet_dist_listen_max),
    try
        application:set_env(kernel, inet_dist_listen_min, Port),
        application:set_env(kernel, inet_dist_listen_max, Port),
        Fun()
    after
        case OldMin of
            undefined -> application:unset_env(kernel, inet_dist_listen_min);
            {ok, V1} -> application:set_env(kernel, inet_dist_listen_min, V1)
        end,
        case OldMax of
            undefined -> application:unset_env(kernel, inet_dist_listen_max);
            {ok, V2} -> application:set_env(kernel, inet_dist_listen_max, V2)
        end
    end.

add_proto(Mod, #s{name = NodeName, listeners = Listeners,
                  acceptors = Acceptors} = State) ->
    case listen_proto(Mod, NodeName) of
        {ok, L = {LSocket, _, _}} ->
            try
                APid = Mod:accept(LSocket),
                true = is_pid(APid),
                {ok, State#s{listeners = [{Mod, L}|Listeners],
                             acceptors = [{APid, Mod}|Acceptors]}}
            catch
                _:E ->
                    catch Mod:close(LSocket),
                    {{error, E}, State}
            end;
        {error, Reason} ->
            error_msg("Ignoring ~p listener, reason: ~p", [Mod, Reason]),
            State
    end.

remove_proto(Mod, #s{listeners = Listeners, acceptors = Acceptors} = State) ->
    info_msg("Closing cb_dist listener ~p", [Mod]),
    {LSocket, _, _} = proplists:get_value(Mod, Listeners),
    [erlang:unlink(P) || {P, M} <- Acceptors, M =:= Mod],
    catch Mod:close(LSocket),
    case lists:member(Mod, [inet_tls_dist, inet6_tls_dist]) of
        true -> exit(whereis(ssl_tls_dist_proxy), restart);
        false -> ok
    end,
    State#s{listeners = proplists:delete(Mod, Listeners),
            acceptors = [{P, M} || {P, M} <- Acceptors, M =/= Mod]}.

listen_proto(Module, NodeName) ->
    NameStr = atom_to_list(NodeName),
    {ok, Port} = cb_epmd:port_for_node(Module, NameStr),
    info_msg("Listening on ~p (~p)", [Port, Module]),
    ListenFun = fun () -> Module:listen(NodeName) end,
    case with_dist_port(Port, ListenFun) of
        {ok, Res} -> {ok, Res};
        Error ->
            error_msg("Failed to start dist ~p on port ~p with reason: ~p",
                      [Module, Port, Error]),
            Error
    end.

can_add_proto(P, #s{listeners = L}) ->
    case is_valid_protocol(P) of
        true ->
            case proplists:is_defined(P, L) of
                false ->
                    HasInet6Tls = proplists:is_defined(inet6_tls_dist, L),
                    HasInetTls = proplists:is_defined(inet_tls_dist, L),
                    case P of
                        inet6_tls_dist when HasInetTls ->
                            {error, {already_has, inet_tls_dist}};
                        inet_tls_dist when HasInet6Tls ->
                            {error, {already_has, inet6_tls_dist}};
                        _ ->
                            ok
                    end;
                true ->
                    {error, already_enabled}
            end;
        false ->
            {error, invalid_protocol}
    end.

is_valid_protocol(P) ->
    lists:member(P, [inet_tcp_dist, inet6_tcp_dist, inet_tls_dist,
                     inet6_tls_dist]).

info_msg(F, A) -> error_logger:info_msg("cb_dist: " ++ F, A).
error_msg(F, A) -> error_logger:error_msg("cb_dist: " ++ F, A).

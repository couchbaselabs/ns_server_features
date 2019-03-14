%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2019 Couchbase, Inc.
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
%% @doc Web server for menelaus.

-module(menelaus_web).

-author('NorthScale <info@northscale.com>').

-behavior(ns_log_categorizing).

-include("menelaus_web.hrl").
-include("ns_common.hrl").
-include("ns_heart.hrl").
-include("ns_stats.hrl").
-include("rbac.hrl").

-export([start_link/0,
         start_link/1,
         stop/0,
         loop/2,
         webconfig/0,
         webconfig/1,
         restart/0,
         get_uuid/0]).

-export([ns_log_cat/1, ns_log_code_string/1]).

-import(menelaus_util,
        [redirect_permanently/2,
         reply/2,
         reply_text/3,
         reply_text/4,
         reply_ok/3,
         reply_json/3,
         reply_not_found/1,
         get_option/2]).

-define(PLUGGABLE_UI, "_p").

%% External API

start_link() ->
    start_link(webconfig()).

start_link(Options) ->
    {AppRoot, Options1} = get_option(approot, Options),
    Plugins = menelaus_pluggable_ui:find_plugins(),
    IsSSL = proplists:get_value(ssl, Options1, false),
    Loop = fun (Req) ->
                   ?MODULE:loop(Req, {AppRoot, IsSSL, Plugins})
           end,
    case mochiweb_http:start_link([{loop, Loop} | Options1]) of
        {ok, Pid} -> {ok, Pid};
        Other ->
            ?MENELAUS_WEB_LOG(?START_FAIL,
                              "Failed to start web service:  ~p~n", [Other]),
            Other
    end.

stop() ->
    % Note that a supervisor might restart us right away.
    mochiweb_http:stop(?MODULE).

restart() ->
    % Depend on our supervision tree to restart us right away.
    stop().

webconfig(Config) ->
    Ip = case os:getenv("MOCHIWEB_IP") of
             false -> misc:inaddr_any();
             Any -> Any
         end,
    Port = case os:getenv("MOCHIWEB_PORT") of
               false ->
                   service_ports:get_port(rest_port, Config);
               P ->
                   list_to_integer(P)
           end,
    WebConfig = [{ip, Ip},
                 {name, ?MODULE},
                 {port, Port},
                 {nodelay, true},
                 {approot, menelaus_deps:local_path(["priv","public"],
                                                    ?MODULE)}],
    WebConfig.

webconfig() ->
    webconfig(ns_config:get()).

loop(Req, Config) ->
    ok = menelaus_sup:barrier_wait(),

    try
        %% Using raw_path so encoded slash characters like %2F are handed correctly,
        %% in that we delay converting %2F's to slash characters until after we split by slashes.
        "/" ++ RawPath = mochiweb_request:get(raw_path, Req),
        {Path, _, _} = mochiweb_util:urlsplit_path(RawPath),
        PathTokens = lists:map(fun mochiweb_util:unquote/1, string:tokens(Path, "/")),

        case is_throttled_request(PathTokens) of
            false ->
                loop_inner(Req, Config, Path, PathTokens);
            true ->
                request_throttler:request(
                  rest,
                  fun () ->
                          loop_inner(Req, Config, Path, PathTokens)
                  end,
                  fun (_Error, Reason) ->
                          Retry = integer_to_list(rand:uniform(10)),
                          reply_text(Req, Reason, 503, [{"Retry-After", Retry}])
                  end)
        end
    catch
        exit:normal ->
            %% this happens when the client closed the connection
            exit(normal);
        throw:{web_exception, StatusCode, Message, ExtraHeaders} ->
            reply_text(Req, Message, StatusCode, ExtraHeaders);
        Type:What ->
            Report = ["web request failed",
                      {path, mochiweb_request:get(path, Req)},
                      {method, mochiweb_request:get(method, Req)},
                      {type, Type}, {what, What},
                      {trace, erlang:get_stacktrace()}], % todo: find a way to enable this for field info gathering
            ?log_error("Server error during processing: ~p", [Report]),
            reply_json(Req, [list_to_binary("Unexpected server error, request logged.")], 500)
    end.

is_throttled_request(["internalSettings"]) ->
    false;
is_throttled_request(["diag" | _]) ->
    false;
is_throttled_request(["couchBase" | _]) ->      % this gets throttled as capi request
    false;
%% this gets throttled via capi too
is_throttled_request(["pools", _, "buckets", _BucketId, "docs"]) ->
    false;
is_throttled_request([?PLUGGABLE_UI | _]) ->
    %% Requests for pluggable UI is not throttled here.
    %% If necessary it is done in the service node.
    false;
is_throttled_request(_) ->
    true.

-type action() :: {done, term()} |
                  {local, fun()} |
                  {ui, boolean(), fun()} |
                  {ui, boolean(), fun(), [term()]} |
                  {rbac_permission() | no_check, fun()} |
                  {rbac_permission() | no_check, fun(), [term()]}.

-spec get_action(mochiweb_request(), {term(), boolean(), term()}, string(), [string()]) -> action().
get_action(Req, {AppRoot, IsSSL, Plugins}, Path, PathTokens) ->
    case mochiweb_request:get(method, Req) of
        Method when Method =:= 'GET'; Method =:= 'HEAD' ->
            case PathTokens of
                [] ->
                    {done, redirect_permanently("/ui/index.html", Req)};
                ["ui"] ->
                    {done, redirect_permanently("/ui/index.html", Req)};
                ["_ui", "canUseCertForAuth"] ->
                    {ui, IsSSL,
                     fun menelaus_web_misc:handle_can_use_cert_for_auth/1};
                ["versions"] ->
                    {done, menelaus_web_misc:handle_versions(Req)};
                ["whoami"] ->
                    {no_check, fun menelaus_web_rbac:handle_whoami/1};
                ["pools"] ->
                    {{[pools], read}, fun menelaus_web_pools:handle_pools/1};
                ["pools", "default"] ->
                    {{[pools], read}, fun menelaus_web_pools:check_and_handle_pool_info/2, ["default"]};
                %% NOTE: see MB-10859. Our docs used to
                %% recommend doing this which due to old
                %% code's leniency worked just like
                %% /pools/default. So temporarily we allow
                %% /pools/nodes to be alias for
                %% /pools/default
                ["pools", "nodes"] ->
                    {{[pools], read}, fun menelaus_web_pools:check_and_handle_pool_info/2, ["default"]};
                ["pools", "default", "overviewStats"] ->
                    {{[{bucket, any}, stats], read}, fun menelaus_stats:handle_overview_stats/2, ["default"]};
                ["_uistats"] ->
                    {{[stats], read}, fun menelaus_stats:serve_ui_stats/1};
                ["_uiEnv"] ->
                    {done, serve_ui_env(Req)};
                ["poolsStreaming", "default"] ->
                    {{[pools], read}, fun menelaus_web_pools:handle_pool_info_streaming/2, ["default"]};
                ["pools", "default", "buckets"] ->
                    {{[{bucket, any}, settings], read}, fun menelaus_web_buckets:handle_bucket_list/1, []};
                ["pools", "default", "saslBucketsStreaming"] ->
                    {{[admin, buckets], read},
                     fun menelaus_web_buckets:handle_sasl_buckets_streaming/2,
                     ["default"]};
                ["pools", "default", "buckets", Id] ->
                    {{[{bucket, Id}, settings], read},
                     fun menelaus_web_buckets:handle_bucket_info/3,
                     ["default", Id]};
                ["pools", "default", "bucketsStreaming", Id] ->
                    {{[{bucket, Id}, settings], read},
                     fun menelaus_web_buckets:handle_bucket_info_streaming/3,
                     ["default", Id]};
                ["pools", "default", "buckets", Id, "ddocs"] ->
                    {{[{bucket, Id}, views], read},
                     fun menelaus_web_buckets:handle_ddocs_list/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "docs"] ->
                    {{[{bucket, Id}, data, docs], read},
                     fun menelaus_web_crud:handle_list/2, [Id]};
                ["pools", "default", "buckets", Id, "docs", DocId] ->
                    {{[{bucket, Id}, data, docs], read},
                     fun menelaus_web_crud:handle_get/3, [Id, DocId]};
                ["pools", "default", "buckets", "@" ++ _ = Id, "stats"] ->
                    {{[stats], read},
                     fun menelaus_stats:handle_stats_section/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "stats"] ->
                    {{[{bucket, Id}, stats], read},
                     fun menelaus_stats:handle_bucket_stats/3,
                     ["default", Id]};
                ["pools", "default", "buckets", Id, "localRandomKey"] ->
                    {{[{bucket, Id}, data, docs], read},
                     fun menelaus_web_buckets:handle_local_random_key/3,
                     ["default", Id]};
                ["pools", "default", "buckets", Id, "statsDirectory"] ->
                    {{[{bucket, Id}, stats], read}, fun menelaus_stats:serve_stats_directory/3,
                     ["default", Id]};
                ["pools", "default", "nodeServices"] ->
                    {{[pools], read}, fun menelaus_web_cluster:serve_node_services/1, []};
                ["pools", "default", "nodeServicesStreaming"] ->
                    {{[pools], read}, fun menelaus_web_cluster:serve_node_services_streaming/1, []};
                ["pools", "default", "b", BucketName] ->
                    {{[{bucket, BucketName}, settings], read},
                     fun menelaus_web_buckets:serve_short_bucket_info/2, [BucketName]};
                ["pools", "default", "bs", BucketName] ->
                    {{[{bucket, BucketName}, settings], read},
                     fun menelaus_web_buckets:serve_streaming_short_bucket_info/2, [BucketName]};
                ["pools", "default", "buckets", Id, "nodes"] ->
                    {{[{bucket, Id}, settings], read},
                     fun menelaus_web_node:handle_bucket_node_list/2, [Id]};
                ["pools", "default", "buckets", Id, "nodes", NodeId] ->
                    {{[{bucket, Id}, settings], read},
                     fun menelaus_web_node:handle_bucket_node_info/3, [Id, NodeId]};
                ["pools", "default", "buckets", "@" ++ _ = Id, "nodes", NodeId,
                 "stats"] ->
                    {{[stats], read},
                     fun menelaus_stats:handle_stats_section_for_node/4,
                     ["default", Id, NodeId]};
                ["pools", "default", "buckets", Id, "nodes", NodeId, "stats"] ->
                    {{[{bucket, Id}, stats], read},
                     fun menelaus_stats:handle_bucket_node_stats/4,
                     ["default", Id, NodeId]};
                ["pools", "default", "buckets", Id, "stats", StatName] ->
                    {{[{bucket, Id}, stats], read},
                     fun menelaus_stats:handle_specific_stat_for_buckets/4,
                     ["default", Id, StatName]};
                ["pools", "default", "buckets", Id, "recoveryStatus"] ->
                    {{[{bucket, Id}, recovery], read},
                     fun menelaus_web_recovery:handle_recovery_status/3,
                     ["default", Id]};
                ["pools", "default", "buckets", Id, "collections"] ->
                    {{[{bucket, Id}, collections], read},
                     fun menelaus_web_collections:handle_get/2, [Id]};
                ["pools", "default", "remoteClusters"] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["pools", "default", "serverGroups"] ->
                    {{[server_groups], read},
                     fun menelaus_web_groups:handle_server_groups/1};
                ["pools", "default", "certificate"] ->
                    {done, menelaus_web_cert:handle_cluster_certificate(Req)};
                ["pools", "default", "certificate", "node", Node] ->
                    {{[admin, security], read},
                     fun menelaus_web_cert:handle_get_node_certificate/2, [Node]};
                ["pools", "default", "settings", "memcached", "global"] ->
                    {{[admin, memcached], read}, fun menelaus_web_mcd_settings:handle_global_get/1};
                ["pools", "default", "settings", "memcached", "effective", Node] ->
                    {{[admin, memcached], read}, fun menelaus_web_mcd_settings:handle_effective_get/2, [Node]};
                ["pools", "default", "settings", "memcached", "node", Node] ->
                    {{[admin, memcached], read}, fun menelaus_web_mcd_settings:handle_node_get/2, [Node]};
                ["pools", "default", "settings", "memcached", "node", Node, "setting", Name] ->
                    {{[admin, memcached], read}, fun menelaus_web_mcd_settings:handle_node_setting_get/3, [Node, Name]};
                ["nodeStatuses"] ->
                    {{[nodes], read}, fun menelaus_web_node:handle_node_statuses/1};
                ["logs"] ->
                    {{[logs], read}, fun menelaus_alert:handle_logs/1};
                ["settings", "web"] ->
                    {{[settings], read}, fun menelaus_web_settings:handle_settings_web/1};
                ["settings", "alerts"] ->
                    {{[settings], read}, fun menelaus_web_settings:handle_settings_alerts/1};
                ["settings", "stats"] ->
                    {{[settings], read}, fun menelaus_web_settings:handle_settings_stats/1};
                ["settings", "autoFailover"] ->
                    {{[settings], read}, fun menelaus_web_auto_failover:handle_settings_get/1};
                ["settings", "autoReprovision"] ->
                    {{[settings], read},
                     fun menelaus_web_settings:handle_settings_auto_reprovision/1};
                ["settings", "querySettings"] ->
                    {{[settings], read}, fun menelaus_web_queries:handle_settings_get/1};
                ["settings", "querySettings", "curlWhitelist"] ->
                    {{[settings], read},
                     fun menelaus_web_queries:handle_curl_whitelist_get/1};
                ["settings", "logRedaction"] ->
                    {{[settings], read}, fun menelaus_web_cluster_logs:handle_settings_log_redaction/1};
                ["settings", "maxParallelIndexers"] ->
                    {{[settings, indexes], read},
                     fun menelaus_web_settings:handle_settings_max_parallel_indexers/1};
                ["settings", "viewUpdateDaemon"] ->
                    {{[settings, indexes], read},
                     fun menelaus_web_settings:handle_settings_view_update_daemon/1};
                ["settings", "autoCompaction"] ->
                    {{[settings], read},
                     fun menelaus_web_autocompaction:handle_get_global_settings/1};
                ["settings", "readOnlyAdminName"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_settings_read_only_admin_name/1};
                ["settings", "replications"] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["settings", "replications", _XID] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["settings", "saslauthdAuth"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_saslauthd_auth_settings/1};
                ["settings", "ldap"] ->
                    {{[admin, security], read},
                     fun menelaus_web_ldap:handle_ldap_settings/1};
                ["settings", "clientCertAuth"] ->
                    {{[admin, security], read},
                     fun menelaus_web_cert:handle_client_cert_auth_settings/1};
                ["settings", "audit"] ->
                    {{[admin, security], read},
                     fun menelaus_web_audit:handle_get/1};
                ["settings", "audit", "descriptors"] ->
                    {{[admin, security], read},
                     fun menelaus_web_audit:handle_get_descriptors/1};
                ["settings", "rbac", "roles"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_roles/1};
                ["settings", "rbac", "users"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_users/2, [Path]};
                ["settings", "rbac", "users", Domain] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_users/3, [Path, Domain]};
                ["settings", "rbac", "users", Domain, UserId] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_user/3, [Domain, UserId]};
                ["settings", "rbac", "groups"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_groups/2, [Path]};
                ["settings", "rbac", "groups", GroupId] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_group/2, [GroupId]};
                ["settings", "rbac", "profiles"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_profiles/1};
                ["settings", "rbac", "profiles", "@self"] ->
                    {no_check,
                     fun menelaus_web_rbac:handle_get_profile/2, [self]};
                ["settings", "rbac", "profiles", Domain, UserId] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_profile/2,
                     [{UserId, Domain}]};
                ["settings", "passwordPolicy"] ->
                    {{[admin, security], read},
                     fun menelaus_web_rbac:handle_get_password_policy/1};
                ["settings", "security"] ->
                    {{[admin, security], read},
                     fun menelaus_web_settings:handle_get/2, [security]};
                ["settings", "license"] ->
                    {{[admin, license], read},
                     fun menelaus_web_license:handle_settings_get/1};
                ["internalSettings"] ->
                    {{[admin, settings], read},
                     fun menelaus_web_settings:handle_get/2, [internal]};
                ["nodes", NodeId] ->
                    {{[nodes], read}, fun menelaus_web_node:handle_node/2, [NodeId]};
                ["nodes", "self", "xdcrSSLPorts"] ->
                    {done, menelaus_web_node:handle_node_self_xdcr_ssl_ports(Req)};
                ["indexStatus"] ->
                    {{[{bucket, any}, n1ql, index], read}, fun menelaus_web_indexes:handle_index_status/1};
                ["settings", "indexes"] ->
                    {{[settings, indexes], read}, fun menelaus_web_indexes:handle_settings_get/1};
                ["diag"] ->
                    {{[admin, diag], read}, fun diag_handler:handle_diag/1, []};
                ["diag", "vbuckets"] ->
                    {{[admin, diag], read}, fun diag_handler:handle_diag_vbuckets/1};
                ["diag", "ale"] ->
                    {{[admin, diag], read}, fun diag_handler:handle_diag_ale/1};
                ["diag", "masterEvents"] ->
                    {{[admin, diag], read}, fun diag_handler:handle_diag_master_events/1};
                ["diag", "password"] ->
                    {local, fun diag_handler:handle_diag_get_password/1};
                ["pools", "default", "rebalanceProgress"] ->
                    {{[tasks], read}, fun menelaus_web_cluster:handle_rebalance_progress/2, ["default"]};
                ["pools", "default", "tasks"] ->
                    {{[tasks], read}, fun menelaus_web_misc:handle_tasks/2, ["default"]};
                ["index.html"] ->
                    {done, redirect_permanently("/ui/index.html", Req)};
                ["ui", "index.html"] ->
                    {ui, IsSSL, fun handle_ui_root/5, [AppRoot, Path, ?VERSION_50,
                                                       Plugins]};
                ["ui", "new-index.html"] ->
                    {ui, IsSSL, fun handle_ui_root/5, [AppRoot, Path, ?VERSION_41,
                                                       []]};
                ["sasl_logs"] ->
                    {{[admin, logs], read}, fun diag_handler:handle_sasl_logs/1, []};
                ["sasl_logs", LogName] ->
                    {{[admin, logs], read}, fun diag_handler:handle_sasl_logs/2, [LogName]};
                ["images" | _] ->
                    {ui, IsSSL, fun handle_serve_file/4, [AppRoot, Path, 30000000]};
                ["couchBase" | _] -> {no_check,
                                      fun menelaus_pluggable_ui:proxy_req/4,
                                      ["couchBase",
                                       drop_prefix(mochiweb_request:get(raw_path, Req)),
                                       Plugins]};
                ["sampleBuckets"] -> {{[samples], read}, fun menelaus_web_samples:handle_get/1};
                ["_metakv" | _] ->
                    {{[admin, internal], all}, fun menelaus_metakv:handle_get/2, [Path]};
                ["_goxdcr", "controller", "bucketSettings", _Bucket] ->
                    XdcrPath = drop_prefix(mochiweb_request:get(raw_path, Req)),
                    {{[admin, internal], all},
                     fun goxdcr_rest:proxy/2, [XdcrPath]};
                ["_cbauth", "checkPermission"] ->
                    {{[admin, internal], all},
                     fun menelaus_web_rbac:handle_check_permission_for_cbauth/1};
                [?PLUGGABLE_UI, "ui", RestPrefix | _] ->
                    {ui, IsSSL, fun menelaus_pluggable_ui:maybe_serve_file/4,
                        [RestPrefix, Plugins, nth_path_tail(Path, 3)]};
                [?PLUGGABLE_UI, RestPrefix | _] ->
                    {no_check,
                     fun (PReq) ->
                             menelaus_pluggable_ui:proxy_req(
                               RestPrefix,
                               drop_rest_prefix(mochiweb_request:get(raw_path, Req)),
                               Plugins, PReq)
                     end};
                _ ->
                    {ui, IsSSL, fun handle_serve_file/4, [AppRoot, Path, 10]}
            end;
        'POST' ->
            case PathTokens of
                ["uilogin"] ->
                    {ui, IsSSL, fun menelaus_web_misc:handle_uilogin/1};
                ["uilogout"] ->
                    {done, menelaus_web_misc:handle_uilogout(Req)};
                ["sampleBuckets", "install"] ->
                    {{[buckets], create}, fun menelaus_web_samples:handle_post/1};
                ["engageCluster2"] ->
                    {{[admin, setup], write}, fun menelaus_web_cluster:handle_engage_cluster2/1};
                ["completeJoin"] ->
                    {{[admin, setup], write}, fun menelaus_web_cluster:handle_complete_join/1};
                ["node", "controller", "doJoinCluster"] ->
                    {{[admin, setup], write}, fun menelaus_web_cluster:handle_join/1};
                ["node", "controller", "doJoinClusterV2"] ->
                    {{[admin, setup], write}, fun menelaus_web_cluster:handle_join/1};
                ["node", "controller", "rename"] ->
                    {{[admin, setup], write}, fun menelaus_web_node:handle_node_rename/1};
                ["nodes", NodeId, "controller", "settings"] ->
                    {{[admin, setup], write}, fun menelaus_web_node:handle_node_settings_post/2,
                     [NodeId]};
                ["node", "controller", "setupServices"] ->
                    {{[admin, setup], write}, fun menelaus_web_cluster:handle_setup_services_post/1};
                ["node", "controller", "reloadCertificate"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_cert:handle_reload_node_certificate/1};
                ["node", "controller", "changeMasterPassword"] ->
                    {{[admin, security], write},
                     fun menelaus_web_secrets:handle_change_master_password/1};
                ["node", "controller", "rotateDataKey"] ->
                    {{[admin, security], write},
                     fun menelaus_web_secrets:handle_rotate_data_key/1};
                ["node", "controller", "setupNetConfig"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_node:handle_setup_net_config/1};
                ["node", "controller", "distProtocols"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_node:handle_dist_protocols/1};
                ["settings", "web"] ->
                    {{[admin, setup], write}, fun menelaus_web_settings:handle_settings_web_post/1};
                ["settings", "alerts"] ->
                    {{[settings], write}, fun menelaus_web_settings:handle_settings_alerts_post/1};
                ["settings", "alerts", "testEmail"] ->
                    {{[settings], write},
                     fun menelaus_web_settings:handle_settings_alerts_send_test_email/1};
                ["settings", "stats"] ->
                    {{[settings], write}, fun menelaus_web_settings:handle_settings_stats_post/1};
                ["settings", "autoFailover"] ->
                    {{[settings], write}, fun menelaus_web_auto_failover:handle_settings_post/1};
                ["settings", "autoFailover", "resetCount"] ->
                    {{[settings], write}, fun menelaus_web_auto_failover:handle_settings_reset_count/1};
                ["settings", "autoReprovision"] ->
                    {{[settings], write},
                     fun menelaus_web_settings:handle_settings_auto_reprovision_post/1};
                ["settings", "querySettings"] ->
                    {{[settings], write}, fun menelaus_web_queries:handle_settings_post/1};
                ["settings", "querySettings", "curlWhitelist"] ->
                    {{[settings], write},
                     fun menelaus_web_queries:handle_curl_whitelist_post/1};
                ["settings", "logRedaction"] ->
                    {{[admin, security], write},
                     fun menelaus_web_cluster_logs:handle_settings_log_redaction_post/1};
                ["settings", "autoReprovision", "resetCount"] ->
                    {{[settings], write},
                     fun menelaus_web_settings:handle_settings_auto_reprovision_reset_count/1};
                ["settings", "maxParallelIndexers"] ->
                    {{[settings, indexes], write},
                     fun menelaus_web_settings:handle_settings_max_parallel_indexers_post/1};
                ["settings", "viewUpdateDaemon"] ->
                    {{[settings, indexes], write},
                     fun menelaus_web_settings:handle_settings_view_update_daemon_post/1};
                ["settings", "readOnlyUser"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_settings_read_only_user_post/1};
                ["settings", "replications"] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["settings", "replications", _XID] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["settings", "saslauthdAuth"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_saslauthd_auth_settings_post/1};
                ["settings", "ldap"] ->
                    {{[admin, security], write},
                     fun menelaus_web_ldap:handle_ldap_settings_post/1};
                ["settings", "ldap", "validate", Type] ->
                    {{[admin, security], write},
                     fun menelaus_web_ldap:handle_ldap_settings_validate_post/2,
                     [Type]};
                ["settings", "invalidateLDAPCache"] ->
                    {{[admin, security], write},
                     fun menelaus_web_ldap:handle_invalidate_ldap_cache/1};
                ["settings", "clientCertAuth"] ->
                    {{[admin, security], write},
                     fun menelaus_web_cert:handle_client_cert_auth_settings_post/1};
                ["settings", "audit"] ->
                    {{[admin, security], write},
                     fun menelaus_web_audit:handle_post/1};
                ["settings", "passwordPolicy"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_post_password_policy/1};
                ["settings", "security"] ->
                    {{[admin, security], write},
                     fun menelaus_web_settings:handle_post/2, [security]};
                ["settings", "developerPreview"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_settings:handle_post/2, [developer_preview]};
                ["settings", "license"] ->
                    {{[admin, license], write},
                     fun menelaus_web_license:handle_settings_post/1};
                ["settings", "license", "validate"] ->
                    {{[admin, license], write},
                     fun menelaus_web_license:handle_settings_validate_post/1};
                ["validateCredentials"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_validate_saslauthd_creds_post/1};
                ["internalSettings"] ->
                    {{[admin, settings], write},
                     fun menelaus_web_settings:handle_post/2, [internal]};
                ["pools", "default"] ->
                    {{[pools], write}, fun menelaus_web_pools:handle_pool_settings_post/1};
                ["controller", "ejectNode"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_eject_post/1};
                ["controller", "addNode"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_add_node/1};
                ["controller", "addNodeV2"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_add_node/1};
                ["pools", "default", "serverGroups", UUID, "addNode"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_add_node_to_group/2, [UUID]};
                ["pools", "default", "serverGroups", UUID, "addNodeV2"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_add_node_to_group/2, [UUID]};
                ["controller", "failOver"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_failover/1};
                ["controller", "startGracefulFailover"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_start_graceful_failover/1};
                ["controller", "rebalance"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_rebalance/1};
                ["controller", "reAddNode"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_re_add_node/1};
                ["controller", "reFailOver"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_re_failover/1};
                ["controller", "stopRebalance"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_stop_rebalance/1};
                ["controller", "setRecoveryType"] ->
                    {{[pools], write}, fun menelaus_web_cluster:handle_set_recovery_type/1};
                ["controller", "setAutoCompaction"] ->
                    {{[settings], write},
                     fun menelaus_web_autocompaction:handle_set_global_settings/1};
                ["controller", "createReplication"] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["controller", "cancelXDCR", _XID] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["controller", "cancelXCDR", XID] ->
                    {no_check, fun goxdcr_rest:proxy/2,
                     [menelaus_util:concat_url_path(
                        ["controller", "cancelXDCR", XID])]};
                ["controller", "resetAlerts"] ->
                    {{[settings], write}, fun menelaus_web_settings:handle_reset_alerts/1};
                ["controller", "regenerateCertificate"] ->
                    {{[admin, security], write},
                     fun menelaus_web_cert:handle_regenerate_certificate/1};
                ["controller", "uploadClusterCA"] ->
                    {{[admin, security], write},
                     fun menelaus_web_cert:handle_upload_cluster_ca/1};
                ["controller", "startLogsCollection"] ->
                    {{[admin, logs], read},
                     fun menelaus_web_cluster_logs:handle_start_collect_logs/1};
                ["controller", "cancelLogsCollection"] ->
                    {{[admin, logs], read},
                     fun menelaus_web_cluster_logs:handle_cancel_collect_logs/1};
                ["controller", "resetAdminPassword"] ->
                    {local, fun menelaus_web_rbac:handle_reset_admin_password/1};
                ["controller", "changePassword"] ->
                    {no_check, fun menelaus_web_rbac:handle_change_password/1};
                ["pools", "default", "buckets", Id] ->
                    {{[{bucket, Id}, settings], write},
                     fun menelaus_web_buckets:handle_bucket_update/3,
                     ["default", Id]};
                ["pools", "default", "buckets"] ->
                    {{[buckets], create},
                     fun menelaus_web_buckets:handle_bucket_create/2,
                     ["default"]};
                ["pools", "default", "buckets", Id, "docs", DocId] ->
                    {{[{bucket, Id}, data, docs], upsert},
                     fun menelaus_web_crud:handle_post/3, [Id, DocId]};
                ["pools", "default", "buckets", Id, "controller", "doFlush"] ->
                    {{[{bucket, Id}], flush},
                     fun menelaus_web_buckets:handle_bucket_flush/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "compactBucket"] ->
                    {{[{bucket, Id}], compact},
                     fun menelaus_web_buckets:handle_compact_bucket/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "unsafePurgeBucket"] ->
                    {{[{bucket, Id}], delete},
                     fun menelaus_web_buckets:handle_purge_compact_bucket/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "cancelBucketCompaction"] ->
                    {{[{bucket, Id}], compact},
                     fun menelaus_web_buckets:handle_cancel_bucket_compaction/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "compactDatabases"] ->
                    {{[{bucket, Id}], compact},
                     fun menelaus_web_buckets:handle_compact_databases/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "cancelDatabasesCompaction"] ->
                    {{[{bucket, Id}], compact},
                     fun menelaus_web_buckets:handle_cancel_databases_compaction/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "startRecovery"] ->
                    {{[{bucket, Id}, recovery], write},
                     fun menelaus_web_recovery:handle_start_recovery/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "stopRecovery"] ->
                    {{[{bucket, Id}, recovery], write},
                     fun menelaus_web_recovery:handle_stop_recovery/3, ["default", Id]};
                ["pools", "default", "buckets", Id, "controller", "commitVBucket"] ->
                    {{[{bucket, Id}, recovery], write},
                     fun menelaus_web_recovery:handle_commit_vbucket/3, ["default", Id]};
                ["pools", "default", "buckets", Id,
                 "ddocs", DDocId, "controller", "compactView"] ->
                    {{[{bucket, Id}, views], compact},
                     fun menelaus_web_buckets:handle_compact_view/4, ["default", Id, DDocId]};
                ["pools", "default", "buckets", Id,
                 "ddocs", DDocId, "controller", "cancelViewCompaction"] ->
                    {{[{bucket, Id}, views], compact},
                     fun menelaus_web_buckets:handle_cancel_view_compaction/4,
                     ["default", Id, DDocId]};
                ["pools", "default", "buckets", Id,
                 "ddocs", DDocId, "controller", "setUpdateMinChanges"] ->
                    {{[{bucket, Id}, views], compact},
                     fun menelaus_web_buckets:handle_set_ddoc_update_min_changes/4,
                     ["default", Id, DDocId]};
                ["pools", "default", "buckets", Id, "collections", Scope] ->
                    {{[{bucket, Id}, collections], write},
                     fun menelaus_web_collections:handle_post_collection/3,
                     [Id, Scope]};
                ["pools", "default", "buckets", Id, "collections"] ->
                    {{[{bucket, Id}, collections], write},
                     fun menelaus_web_collections:handle_post_scope/2, [Id]};
                ["pools", "default", "remoteClusters"] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["pools", "default", "remoteClusters", _Id] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["pools", "default", "serverGroups"] ->
                    {{[server_groups], write},
                     fun menelaus_web_groups:handle_server_groups_post/1};
                ["pools", "default", "settings", "memcached", "global"] ->
                    {{[admin, memcached], write},
                     fun menelaus_web_mcd_settings:handle_global_post/1};
                ["pools", "default", "settings", "memcached", "node", Node] ->
                    {{[admin, memcached], write},
                     fun menelaus_web_mcd_settings:handle_node_post/2, [Node]};
                ["pools", "default", "checkPermissions"] ->
                    {no_check,
                     fun menelaus_web_rbac:handle_check_permissions_post/1};
                ["settings", "indexes"] ->
                    {{[settings, indexes], write}, fun menelaus_web_indexes:handle_settings_post/1};
                ["_cbauth"] ->
                    {no_check, fun menelaus_cbauth:handle_cbauth_post/1};
                ["_cbauth", "extractUserFromCert"] ->
                    {{[admin, internal], all},
                     fun menelaus_cbauth:handle_extract_user_from_cert_post/1};
                ["_log"] ->
                    {{[admin, internal], all}, fun menelaus_web_misc:handle_log_post/1};
                ["_goxdcr", "regexpValidation"] ->
                    {no_check, fun goxdcr_rest:proxy/2,
                     [menelaus_util:concat_url_path(
                        ["controller", "regexpValidation"])]};
                ["_goxdcr", "controller", "bucketSettings", _Bucket] ->
                    XdcrPath = drop_prefix(mochiweb_request:get(raw_path, Req)),
                    {{[admin, internal], all},
                     fun goxdcr_rest:proxy/2, [XdcrPath]};
                ["_goxdcr", "_pre_replicate", Bucket] ->
                    {{[{bucket, Bucket}, data, meta], read},
                     fun menelaus_web_xdcr_target:handle_pre_replicate/2,
                     [Bucket]};
                ["logClientError"] ->
                    {no_check, fun log_client_error/1};
                ["diag", "eval"] ->
                    {{[admin, diag], write}, fun diag_handler:handle_diag_eval/1};
                ["couchBase" | _] ->
                    {no_check, fun menelaus_pluggable_ui:proxy_req/4,
                     ["couchBase",
                      drop_prefix(mochiweb_request:get(raw_path, Req)),
                      Plugins]};
                [?PLUGGABLE_UI, RestPrefix | _] ->
                    {no_check,
                     fun (PReq) ->
                             menelaus_pluggable_ui:proxy_req(
                               RestPrefix,
                               drop_rest_prefix(mochiweb_request:get(raw_path, Req)),
                               Plugins, PReq)
                     end};
                _ ->
                    ?MENELAUS_WEB_LOG(0001, "Invalid post received: ~p", [Req]),
                    {done, reply_not_found(Req)}
            end;
        'DELETE' ->
            case PathTokens of
                ["pools", "default", "buckets", Id] ->
                    {{[{bucket, Id}], delete},
                     fun menelaus_web_buckets:handle_bucket_delete/3, ["default", Id]};
                ["pools", "default", "remoteClusters", _Id] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["pools", "default", "buckets", Id, "collections", Name] ->
                    {{[{bucket, Id}, collections], delete},
                     fun menelaus_web_collections:handle_delete_scope/3,
                     [Id, Name]};
                ["pools", "default", "buckets", Id, "collections",
                 Scope, Name] ->
                    {{[{bucket, Id}, collections], delete},
                     fun menelaus_web_collections:handle_delete_collection/4,
                     [Id, Scope, Name]};
                ["pools", "default", "buckets", Id, "docs", DocId] ->
                    {{[{bucket, Id}, data, docs], delete},
                     fun menelaus_web_crud:handle_delete/3, [Id, DocId]};
                ["controller", "cancelXCDR", XID] ->
                    {no_check, fun goxdcr_rest:proxy/2,
                     [menelaus_util:concat_url_path(
                        ["controller", "cancelXDCR", XID])]};
                ["controller", "cancelXDCR", _XID] ->
                    {no_check, fun goxdcr_rest:proxy/1};
                ["settings", "readOnlyUser"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_read_only_user_delete/1};
                ["pools", "default", "serverGroups", GroupUUID] ->
                    {{[server_groups], write},
                     fun menelaus_web_groups:handle_server_group_delete/2, [GroupUUID]};
                ["pools", "default", "settings", "memcached", "node", Node, "setting", Name] ->
                    {{[admin, memcached], write},
                     fun menelaus_web_mcd_settings:handle_node_setting_delete/3, [Node, Name]};
                ["settings", "rbac", "users", UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_delete_user/3, ["external", UserId]};
                ["settings", "rbac", "users", Domain, UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_delete_user/3, [Domain, UserId]};
                ["settings", "rbac", "groups", GroupId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_delete_group/2, [GroupId]};
                ["settings", "rbac", "profiles", "@self"] ->
                    {no_check,
                     fun menelaus_web_rbac:handle_delete_profile/2, [self]};
                ["settings", "rbac", "profiles", Domain, UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_delete_profile/2,
                     [{UserId, Domain}]};
                ["couchBase" | _] -> {no_check,
                                      fun menelaus_pluggable_ui:proxy_req/4,
                                      ["couchBase",
                                       drop_prefix(mochiweb_request:get(raw_path, Req)),
                                       Plugins]};
                ["_metakv" | _] ->
                    {{[admin, internal], all}, fun menelaus_metakv:handle_delete/2, [Path]};
                [?PLUGGABLE_UI, RestPrefix | _] ->
                    {no_check,
                     fun (PReq) ->
                             menelaus_pluggable_ui:proxy_req(
                               RestPrefix,
                               drop_rest_prefix(mochiweb_request:get(raw_path, Req)),
                               Plugins, PReq)
                     end};
                ["node", "controller", "setupAlternateAddresses", "external"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_node:handle_node_altaddr_external_delete/1};
                _ ->
                    ?MENELAUS_WEB_LOG(0002, "Invalid delete received: ~p as ~p",
                                      [Req, PathTokens]),
                    {done, reply_text(Req, "Method Not Allowed", 405)}
            end;
        'PUT' = Method ->
            case PathTokens of
                ["settings", "readOnlyUser"] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_read_only_user_reset/1};
                ["pools", "default", "serverGroups"] ->
                    {{[server_groups], write},
                     fun menelaus_web_groups:handle_server_groups_put/1};
                ["pools", "default", "serverGroups", GroupUUID] ->
                    {{[server_groups], write},
                     fun menelaus_web_groups:handle_server_group_update/2, [GroupUUID]};
                ["settings", "rbac", "users", UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_put_user/3, ["external", UserId]};
                ["settings", "rbac", "users", Domain, UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_put_user/3, [Domain, UserId]};
                ["settings", "rbac", "groups", GroupId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_put_group/2, [GroupId]};
                ["settings", "rbac", "profiles", "@self"] ->
                    {no_check,
                     fun menelaus_web_rbac:handle_put_profile/2, [self]};
                ["settings", "rbac", "profiles", Domain, UserId] ->
                    {{[admin, security], write},
                     fun menelaus_web_rbac:handle_put_profile/2,
                     [{UserId, Domain}]};
                ["couchBase" | _] ->
                    {no_check, fun menelaus_pluggable_ui:proxy_req/4,
                     ["couchBase",
                      drop_prefix(mochiweb_request:get(raw_path, Req)),
                      Plugins]};
                ["_metakv" | _] ->
                    {{[admin, internal], all}, fun menelaus_metakv:handle_put/2, [Path]};
                [?PLUGGABLE_UI, RestPrefix | _] ->
                    {no_check,
                     fun (PReq) ->
                             menelaus_pluggable_ui:proxy_req(
                               RestPrefix,
                               drop_rest_prefix(mochiweb_request:get(raw_path, Req)),
                               Plugins, PReq)
                     end};
                ["node", "controller", "setupAlternateAddresses", "external"] ->
                    {{[admin, setup], write},
                     fun menelaus_web_node:handle_node_altaddr_external/1};
                _ ->
                    ?MENELAUS_WEB_LOG(0003, "Invalid ~p received: ~p", [Method, Req]),
                    {done, reply_text(Req, "Method Not Allowed", 405)}
            end;
        "RPCCONNECT" ->
            {{[admin, internal], all}, fun json_rpc_connection_sup:handle_rpc_connect/1};

        _ ->
            ?MENELAUS_WEB_LOG(0004, "Invalid request received: ~p", [Req]),
            {done, reply_text(Req, "Method Not Allowed", 405)}
    end.

log_client_error(Req) ->
    Body = case mochiweb_request:recv_body(Req) of
               undefined ->
                   "(nothing)";
               B ->
                   binary_to_list(B)
           end,

    User = case menelaus_auth:get_user_id(Req) of
               [] ->
                   "(anonymous)";
               UserName ->
                   UserName
           end,

    ?MENELAUS_WEB_LOG(
       ?UI_SIDE_ERROR_REPORT,
       "Client-side error-report for user ~p on node ~p:~nUser-Agent:~s~n~s~n",
       [User, node(), mochiweb_request:get_header_value("user-agent", Req), Body]),
    reply_ok(Req, "text/plain", []).

serve_ui(Req, IsSSL, F, Args) ->
    IsDisabledKey = case IsSSL of
                        true ->
                            disable_ui_over_https;
                        false ->
                            disable_ui_over_http
                    end,
    case ns_config:read_key_fast(IsDisabledKey, false) of
        true ->
            reply(Req, 404);
        false ->
            apply(F, Args ++ [Req])
    end.

use_minified(Req) ->
    Query = mochiweb_request:parse_qs(Req),
    %% explicity specified minified in the query params
    %% overrides the application env value
    Minified = proplists:get_value("minified", Query),
    Minified =:= "true" orelse
        Minified =:= undefined andalso
        misc:get_env_default(use_minified, true).

serve_ui_env(Req) ->
    %% UI env values are expected to be unfolded proplists
    UIEnvDefault = lists:ukeysort(1, misc:get_env_default(ui_env, [])),
    GlobalUIEnv = lists:ukeysort(1, ns_config:read_key_fast(ui_env, [])),
    NodeSpecificUIEnv = lists:ukeysort(1, ns_config:read_key_fast({node, node(), ui_env}, [])),
    menelaus_util:reply_json(Req,
                             {lists:ukeymerge(1, NodeSpecificUIEnv,
                                              lists:ukeymerge(1, GlobalUIEnv, UIEnvDefault))}).

handle_ui_root(AppRoot, Path, ?VERSION_50, Plugins, Req) ->
    Filename = case use_minified(Req) of
                   true ->
                       filename:join([AppRoot, "ui", "index.min.html"]);
                   _ ->
                       filename:join(AppRoot, Path)
               end,
    menelaus_util:reply_ok(
      Req,
      "text/html; charset=utf8",
      menelaus_pluggable_ui:inject_head_fragments(Filename, ?VERSION_50, Plugins));
handle_ui_root(AppRoot, Path, ?VERSION_41, [], Req) ->
    menelaus_util:serve_static_file(Req, {AppRoot, Path},
                                    "text/html; charset=utf8", []).

handle_serve_file(AppRoot, Path, MaxAge, Req) ->
    menelaus_util:serve_file(
        Req, Path, AppRoot,
        [{"Cache-Control", lists:concat(["max-age=", MaxAge])}]).

loop_inner(Req, Info, Path, PathTokens) ->
    menelaus_auth:assert_no_meta_headers(Req),
    perform_action(Req, get_action(Req, Info, Path, PathTokens)).

-spec get_bucket_id(rbac_permission() | no_check) -> bucket_name() | false.
get_bucket_id(no_check) ->
    false;
get_bucket_id({Object, _Operations}) ->
    case lists:keyfind(bucket, 1, Object) of
        {bucket, any} ->
            false;
        {bucket, Bucket} ->
            Bucket;
        false ->
            false
    end.

-spec perform_action(mochiweb_request(), action()) -> term().
perform_action(_Req, {done, RV}) ->
    RV;
perform_action(Req, {local, Fun}) ->
    {RV, Headers} = menelaus_auth:verify_local_token(Req),
    NewReq = menelaus_auth:apply_headers(Req, Headers),
    case RV of
        allowed ->
            Fun(NewReq);
        auth_failure ->
            menelaus_util:require_auth(NewReq)
    end;
perform_action(Req, {ui, IsSSL, Fun}) ->
    perform_action(Req, {ui, IsSSL, Fun, []});
perform_action(Req, {ui, IsSSL, Fun, Args}) ->
    serve_ui(Req, IsSSL, Fun, Args);
perform_action(Req, {Permission, Fun}) ->
    perform_action(Req, {Permission, Fun, []});
perform_action(Req, {Permission, Fun, Args}) ->
    {RV, Headers} = menelaus_auth:verify_rest_auth(Req, Permission),
    NewReq = menelaus_auth:apply_headers(Req, Headers),
    case RV of
        allowed ->
            case get_bucket_id(Permission) of
                false ->
                    check_uuid(Fun, Args, NewReq);
                Bucket ->
                    check_bucket_uuid(Bucket, fun check_uuid/3, [Fun, Args],
                                      NewReq)
            end;
        auth_failure ->
            menelaus_util:require_auth(NewReq);
        forbidden ->
            menelaus_util:reply_json(
              NewReq, menelaus_web_rbac:forbidden_response(Permission), 403)
    end.

check_uuid(F, Args, Req) ->
    ReqUUID0 = proplists:get_value("uuid", mochiweb_request:parse_qs(Req)),
    case ReqUUID0 =/= undefined of
        true ->
            ReqUUID = list_to_binary(ReqUUID0),
            UUID = get_uuid(),
            %%
            %% get_uuid() will return empty UUID if the system is not
            %% provisioned yet. If ReqUUID is also empty then we let
            %% the request go through. But, if ReqUUID is not-empty
            %% and UUID is empty then we will retrun 404 error.
            %%
            case ReqUUID =:= UUID of
                true ->
                    erlang:apply(F, Args ++ [Req]);
                false ->
                    reply_text(Req, "Cluster uuid does not match the requested.\r\n", 404)
            end;
        false ->
            erlang:apply(F, Args ++ [Req])
    end.

check_bucket_uuid(Bucket, F, Args, Req) ->
    case ns_bucket:get_bucket(Bucket) of
        not_present ->
            ?log_debug("Attempt to access non existent bucket ~p", [Bucket]),
            reply_not_found(Req);
        {ok, BucketConfig} ->
            menelaus_web_buckets:checking_bucket_uuid(
              Req, BucketConfig,
              fun () ->
                      erlang:apply(F, Args ++ [Req])
              end)
    end.

%% Returns an UUID from the ns_config
%% cluster UUID is set in ns_config only when the system is provisioned.
get_uuid() ->
    case ns_config:search(uuid) of
        false ->
            <<>>;
        {value, Uuid2} ->
            Uuid2
    end.

%% log categorizing, every logging line should be unique, and most
%% should be categorized

ns_log_cat(0013) ->
    crit;
ns_log_cat(0019) ->
    warn;
ns_log_cat(?START_FAIL) ->
    crit;
ns_log_cat(?NODE_EJECTED) ->
    info;
ns_log_cat(?UI_SIDE_ERROR_REPORT) ->
    warn.

ns_log_code_string(0013) ->
    "node join failure";
ns_log_code_string(0019) ->
    "server error during request processing";
ns_log_code_string(?START_FAIL) ->
    "failed to start service";
ns_log_code_string(?NODE_EJECTED) ->
    "node was ejected";
ns_log_code_string(?UI_SIDE_ERROR_REPORT) ->
    "client-side error report".

nth_path_tail(Path, N) when N > 0 ->
    nth_path_tail(path_tail(Path), N-1);
nth_path_tail(Path, 0) ->
    Path.

path_tail([$/|[$/|_] = Path]) ->
    path_tail(Path);
path_tail([$/|Path]) ->
    Path;
path_tail([_|Rest]) ->
    path_tail(Rest);
path_tail([]) ->
    [].

drop_rest_prefix("/" ++ Path) ->
    [$/ | nth_path_tail(Path, 2)].

drop_prefix("/" ++ Path) ->
    [$/ | nth_path_tail(Path, 1)].

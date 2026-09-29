%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_auth).
-moduledoc """
Authorization for remote tier requests.

A behaviour and a dispatcher, separate from the API backend because the two vary
independently. The S3/GCS XML API is one wire protocol that several stores
accept. How a request proves its identity differs per store.

The callback is "authorize this request", not "return credentials". Schemes
disagree on what authorization needs. SigV4 derives its header from the method,
path and payload digest. An OAuth bearer token uses none of them and has no key
pair to return. A credential-shaped callback would put SigV4's shape into every
call site.
""".

-export([start_link/0, authorize/2]).

-type req_headers() :: #{binary() => binary()}.
-type request() :: #{
    method := binary(),
    path := binary(),
    %% `no_body` when the client sends the payload after the headers. There is
    %% nothing to digest at that point.
    body := iodata() | no_body,
    opts := map()
}.
-export_type([req_headers/0, request/0]).

-doc """
Return the headers to send, authorization included.

A backend can add any header its scheme covers, not only `authorization`. SigV4
also sets `host`, `x-amz-date` and the payload digest, because the signature
covers them.
""".
-callback authorize(request(), req_headers()) -> {ok, req_headers()} | {error, term()}.

-doc """
Start the backend's process, or return `ignore` when it needs none.

A scheme that signs each request from static configuration holds nothing to
refresh. It runs no process.
""".
-callback start_link() -> {ok, pid()} | ignore | {error, term()}.

-spec backend() -> module().
backend() ->
    rabbitmq_stream_s3_config:auth_backend().

-doc """
Start the configured auth backend.

An API backend with requests to authorize calls this. The backend module stays
private to this interface.
""".
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    (backend()):start_link().

-spec authorize(request(), req_headers()) -> {ok, req_headers()} | {error, term()}.
authorize(Request, Headers) when is_map(Request) andalso is_map(Headers) ->
    (backend()):authorize(Request, Headers).

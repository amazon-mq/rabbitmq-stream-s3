%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_fault).
-moduledoc """
A fault-injecting S3 API backend for tests.

Wraps `rabbitmq_stream_s3_api_fs`, delegating every operation by default, and
consults a control table first so a test can deterministically block or fail a
specific operation. The control surface (minimal for now):

- `block_once(Op, KeyPattern)` parks the next matching call to `Op` until the
  test releases it. This lets a test freeze the pipeline at a precise point -
  e.g. hold a fragment upload at `stream_put` (before it reads the local
  segment), trim that segment via retention, then release to reproduce the
  trimmed-segment upload failure of issue #225 without a timing race.
- `fail_next(Op, KeyPattern, Reason)` makes the next matching call to `Op`
  return `{error, Reason}` (e.g. `slow_down`, `not_found`, `timeout`).

Operation is one of: get, get_range, get_range_async, put, stream_put. Key
matching is a binary substring (`binary:match/2`). When the control table is
absent (the backend is not configured for a test) every call is a pure
passthrough to the FS backend.
""".

-behaviour(rabbitmq_stream_s3_api).

%% rabbitmq_stream_s3_api behaviour
-export([
    get/2,
    get_range/3,
    get_range_async/3,
    put/3,
    stream_put/3,
    stream_data/2,
    stream_finish/2,
    stream_abort/1,
    delete/2,
    list/3,
    match_async/3,
    handle_async/3,
    cancel_async/2
]).

%% Test control surface
-export([
    setup/0,
    reset/0,
    block_once/2,
    await_blocked/2,
    release/2,
    fail_next/3
]).

-define(TBL, ?MODULE).
-define(FS, rabbitmq_stream_s3_api_fs).

%%----------------------------------------------------------------------------
%% Test control
%%----------------------------------------------------------------------------

-spec setup() -> ok.
setup() ->
    case ets:info(?TBL) of
        undefined -> _ = ets:new(?TBL, [named_table, public, set]);
        _ -> reset()
    end,
    ok.

-spec reset() -> ok.
reset() ->
    catch ets:delete_all_objects(?TBL),
    ok.

%% Arm a one-shot block: the next call to Op whose key matches KeyPat parks
%% until released. Returns a Ref to await/release on.
-spec block_once(atom(), binary()) -> reference().
block_once(Op, KeyPat) ->
    Ref = make_ref(),
    true = ets:insert(?TBL, {{block, Op}, KeyPat, self(), Ref}),
    Ref.

%% Block until the armed block is hit; returns the blocked (task) pid.
-spec await_blocked(reference(), timeout()) -> pid().
await_blocked(Ref, Timeout) ->
    receive
        {fault_blocked, Ref, TaskPid} -> TaskPid
    after Timeout ->
        error({fault_block_not_hit, Ref})
    end.

%% Release a blocked task so it proceeds to delegate to the FS backend.
-spec release(pid(), reference()) -> ok.
release(TaskPid, Ref) ->
    TaskPid ! {fault_release, Ref},
    ok.

%% Arm a one-shot failure for the next matching call to Op.
-spec fail_next(atom(), binary(), term()) -> ok.
fail_next(Op, KeyPat, Reason) ->
    true = ets:insert(?TBL, {{fail, Op}, KeyPat, Reason, 1}),
    ok.

%%----------------------------------------------------------------------------
%% Behaviour callbacks
%%----------------------------------------------------------------------------

get(Key, Opts) -> with_faults(get, Key, fun() -> ?FS:get(Key, Opts) end).

get_range(Key, RangeSpec, Opts) ->
    with_faults(get_range, Key, fun() -> ?FS:get_range(Key, RangeSpec, Opts) end).

get_range_async(Key, RangeSpec, Opts) ->
    maybe_block(get_range_async, Key),
    case maybe_fail(get_range_async, Key) of
        {error, Reason} ->
            %% Deliver the error on the async channel, mirroring how the FS
            %% backend reports a result, so the reader's retry path engages (a
            %% synchronous {error, _} return is a different, rarer path).
            Req = make_ref(),
            self() ! {'$async', Req, {done, {error, Reason}}},
            {ok, Req, undefined};
        ok ->
            ?FS:get_range_async(Key, RangeSpec, Opts)
    end.

put(Key, Data, Opts) -> with_faults(put, Key, fun() -> ?FS:put(Key, Data, Opts) end).

stream_put(Key, ContentLength, Opts) ->
    with_faults(stream_put, Key, fun() -> ?FS:stream_put(Key, ContentLength, Opts) end).

%% Pure delegation: faults are injected at the operation boundaries above.
stream_data(State, Data) -> ?FS:stream_data(State, Data).
stream_finish(State, Crc) -> ?FS:stream_finish(State, Crc).
stream_abort(State) -> ?FS:stream_abort(State).
delete(Keys, Opts) -> ?FS:delete(Keys, Opts).
list(Prefix, Continuation, Opts) -> ?FS:list(Prefix, Continuation, Opts).
match_async(Msg, Reqs, Cancelled) -> ?FS:match_async(Msg, Reqs, Cancelled).
handle_async(Msg, Req, State) -> ?FS:handle_async(Msg, Req, State).
cancel_async(Req, State) -> ?FS:cancel_async(Req, State).

%%----------------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------------

with_faults(Op, Key, Delegate) ->
    maybe_block(Op, Key),
    case maybe_fail(Op, Key) of
        {error, _} = Err -> Err;
        ok -> Delegate()
    end.

maybe_block(Op, Key) ->
    case lookup({block, Op}) of
        {ok, {KeyPat, TestPid, Ref}} ->
            case matches(KeyPat, Key) of
                true ->
                    %% One-shot: remove before parking so only this call blocks.
                    ets:delete(?TBL, {block, Op}),
                    TestPid ! {fault_blocked, Ref, self()},
                    receive
                        {fault_release, Ref} -> ok
                    end;
                false ->
                    ok
            end;
        none ->
            ok
    end.

maybe_fail(Op, Key) ->
    case lookup({fail, Op}) of
        {ok, {KeyPat, Reason, N}} ->
            case matches(KeyPat, Key) of
                true ->
                    case N =< 1 of
                        true -> ets:delete(?TBL, {fail, Op});
                        false -> ets:insert(?TBL, {{fail, Op}, KeyPat, Reason, N - 1})
                    end,
                    {error, Reason};
                false ->
                    ok
            end;
        none ->
            ok
    end.

lookup(K) ->
    try ets:lookup(?TBL, K) of
        [{_, A, B, C}] -> {ok, {A, B, C}};
        [] -> none
    catch
        error:badarg -> none
    end.

matches(KeyPat, Key) ->
    binary:match(Key, KeyPat) =/= nomatch.

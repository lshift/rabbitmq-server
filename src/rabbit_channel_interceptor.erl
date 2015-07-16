%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_channel_interceptor).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([init/1, intercept_in/3]).

-ifdef(use_specs).

-type(method_name() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).
-type(original_content() :: rabbit_types:content()).
-type(processed_content() :: rabbit_types:content()).

-callback description() -> [proplists:property()].
%% Derive some initial state from the channel. This will be passed back
%% as the third argument of intercept/3.
-callback init(rabbit_channel:channel()) -> any.
-callback intercept(original_method(), original_content(), any) ->
    {processed_method(), processed_content()} | rabbit_misc:channel_or_connection_exit().
-callback applies_to() -> list(method_name()).

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {init, 1}, {intercept, 3}, {applies_to, 0}];
behaviour_info(_Other) ->
    undefined.

-endif.

init(Ch) ->
    Mods = [M || {_, M} <- rabbit_registry:lookup_all(channel_interceptor)],
    check_no_overlap(Mods),
    to_fn(Mods, Ch).

%% Turn a list of interceptor modules into a single function
to_fn(Mods, Ch) ->
    case Mods of
        [] -> fun(M, C) -> {M, C} end;
        [Mod] -> to_fn1(Mod, Ch);
        [Mod|Mods1] ->
            Fn1 = to_fn1(Mod, Ch),
            Fn2 = to_fn(Mods1, Ch),
            fun(M, C) ->
                {M1, C1} = Fn1(M, C),
                Fn2(M1, C1)
            end
    end.

%% Turn a single interceptor module into a function
to_fn1(Mod, Ch) ->
    St = Mod:init(Ch),
    fun (M, C) ->
        % this little dance is because Mod might be unloaded at any point
        case (catch {ok, Mod:intercept(M, C, St)}) of
            {ok, R = {M2, _}} ->
                case validate_method(M, M2) of
                    true -> R;
                    _ ->
                        internal_error("Interceptor: ~p expected to return "
                                            "method: ~p but returned: ~p",
                                       [Mod, rabbit_misc:method_record_type(M),
                                        rabbit_misc:method_record_type(M2)])
                end;
            {'EXIT', {undef, [{Mod, intercept, _, _} | _]}} -> {M, C}
        end
    end.

check_no_overlap(Mods) ->
    check_no_overlap1([sets:from_list(Mod:applies_to()) || Mod <- Mods]).

%% Check no non-empty pairwise intersection in a list of sets
check_no_overlap1(Sets) ->
    lists:foldl(fun(Set, Union) ->
                    Is = sets:intersection(Set, Union),
                    case sets:size(Is) of
                      0 -> ok;
                      _ ->
                        internal_error("Interceptor: more than one "
                                            "module handles ~p~n", [Is])
                    end,
                    sets:union(Set, Union)
                end,
                sets:new(),
                Sets),
    ok.

intercept_in(#'basic.publish'{} = M, C, S) -> intercept_in1(M, C, S);
intercept_in(#'basic.ack'{} = M, C, S) -> intercept_in1(M, C, S);
intercept_in(#'basic.nack'{} = M, C, S) -> intercept_in1(M, C, S);
intercept_in(#'basic.reject'{} = M, C, S) -> intercept_in1(M, C, S);
intercept_in(#'basic.credit'{} = M, C, S) -> intercept_in1(M, C, S);
intercept_in(M, C, S) -> intercept_in1(M, C, S).

intercept_in1(M, C, Fn) -> Fn(M, C).

validate_method(M, M2) ->
    rabbit_misc:method_record_type(M) =:= rabbit_misc:method_record_type(M2).

%% keep dialyzer happy
-spec internal_error(string(), [any()]) -> no_return().
internal_error(Format, Args) ->
    rabbit_misc:protocol_error(internal_error, Format, Args).

-module(rabbit_channel_interceptor_new).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([init/1, intercept_in/3]).

-define(OLD_REGISTRY_CLASS, channel_interceptor).
-define(NEW_REGISTRY_CLASS, channel_interceptor_new).

-ifdef(use_specs).

-type(method_name() :: rabbit_framing:amqp_method_name()).
-type(original_method() :: rabbit_framing:amqp_method_record()).
-type(processed_method() :: rabbit_framing:amqp_method_record()).
-type(original_content() :: rabbit_types:content()).
-type(processed_content() :: rabbit_types:content()).

% Derive some initial state from the channel. This will be passed back
% as the third argument of intercept/3.
-callback init(rabbit_channel:channel()) -> any.
-callback intercept(original_method(), original_content(), any) ->
    {processed_method(), processed_content()} | rabbit_misc:channel_or_connection_exit().
-callback applies_to(method_name()) -> boolean().

-else.

% TODO

-endif.

init(Ch) ->
  R = (catch begin
    Mods = lookup_all(?OLD_REGISTRY_CLASS) ++ lookup_all(?NEW_REGISTRY_CLASS),
    check_no_overlap(Mods),
    {Hot, Cold} = lists:partition(fun ({_, M}) -> is_hot(M) end, Mods),
    {to_fn(Hot, Ch), to_fn(Cold, Ch)}
  end),
  %io:format("R=~p~n", [R]),
  R.

lookup_all(Class) ->
  [{Class, M} || {_, M} <- rabbit_registry:lookup_all(Class)].

% Turn a list of interceptor modules into a single function

to_fn(Mods, Ch) ->
  case Mods of
    [] -> fun(M, C) -> {M, C} end;
    [Mod] -> to_fn1(Mod, Ch)
    [Mod|Mods1] ->
      Fn1 = to_fn1(Mod, Ch),
      Fn2 = to_fn(Mods1, Ch),
      fun(M, C) ->
          {M1, C1} = Fn1(M, C),
          Fn2(M1, C1)
      end
  end.

% Turn a single interceptor module into a function

to_fn1({channel_interceptor, Mod}, Ch) ->
  VHost = rabbit_channel:get_vhost(Ch),
  fun (M, C) -> {Mod:intercept(M, VHost), C} end;

to_fn1({channel_interceptor_new, Mod}, Ch) ->
  St = Mod:init(Ch),
  fun (M, C) -> Mod:intercept(M, C, St) end.

check_no_overlap(Mods) ->
  check_no_overlap1([get_applicable_methods(Mod) || Mod <- Mods]).

check_no_overlap1(Sets) ->
  lists:foldl(fun(Set, Union) ->
                  0 = gb_sets:size(gb_sets:intersection(Set, Union)),
                  gb_sets:union(Set, Union)
              end,
              gb_sets:empty(),
              Sets),
  ok.

get_applicable_methods({_, Mod}) ->
  Methods = ['basic.publish', 'basic.ack', 'basic.nack', 'basic.reject',
             'basic.credit'],
  gb_sets:from_list([Method || Method <- Methods, Mod:applies_to(Method)]).

is_hot(M) ->
  M:applies_to('basic.publish') orelse
  M:applies_to('basic.ack') orelse
  M:applies_to('basic.nack') orelse
  M:applies_to('basic.reject') orelse
  M:applies_to('basic.credit').

intercept_in(#'basic.publish'{} = M, C, S) -> intercept_in_hot(M, C, S);
intercept_in(#'basic.ack'{} = M, C, S) -> intercept_in_hot(M, C, S);
intercept_in(#'basic.nack'{} = M, C, S) -> intercept_in_hot(M, C, S);
intercept_in(#'basic.reject'{} = M, C, S) -> intercept_in_hot(M, C, S);
intercept_in(#'basic.credit'{} = M, C, S) -> intercept_in_hot(M, C, S);
intercept_in(M, C, S) -> intercept_in_cold(M, C, S).

intercept_in_hot(M, C, {Hot, _}) -> apply(Hot, [M, C]).

intercept_in_cold(M, C, {_, Cold}) -> apply(Cold, [M, C]).


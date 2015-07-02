-module(rabbit_channel_interceptor_new).

-include("rabbit_framing.hrl").
-include("rabbit.hrl").

-export([init/1, intercept_in/2]).

-define(OLD_REGISTRY_CLASS, channel_interceptor).

init(Ch) ->
  {get_fn(Ch, hot), get_fn(Ch, cold)}.

get_fn(Ch, Temperature) ->
  VHost = rabbit_channel:get_vhost(Ch),
  Modules = [M || {_, M} <- rabbit_registry:lookup_all(?OLD_REGISTRY_CLASS),
                  has_temperature(M, Temperature)],
  io:format("modules for ~p = ~p~n", [Temperature, Modules]),
  case Modules of
    [] -> fun (Method) -> Method end;
    [Module] -> fun (Method) -> Module:intercept(Method, VHost) end
  end.

has_temperature(M, hot) ->
  io:format("has_temperature ~p~n", [M]),
  M:applies_to('basic.publish') orelse
  M:applies_to('basic.ack') orelse
  M:applies_to('basic.nack') orelse
  M:applies_to('basic.reject') orelse
  M:applies_to('basic.credit');

has_temperature(M, cold) ->
  not has_temperature(M, hot).

intercept_in(#'basic.publish'{} = M, S) -> intercept_in_hot(M, S);
intercept_in(#'basic.ack'{} = M, S) -> intercept_in_hot(M, S);
intercept_in(#'basic.nack'{} = M, S) -> intercept_in_hot(M, S);
intercept_in(#'basic.reject'{} = M, S) -> intercept_in_hot(M, S);
intercept_in(#'basic.credit'{} = M, S) -> intercept_in_hot(M, S);
intercept_in(M, S) -> intercept_in_cold(M, S).

intercept_in_hot(M, {Hot, _}) -> apply(Hot, [M]).

intercept_in_cold(M, {_, Cold}) -> apply(Cold, [M]).


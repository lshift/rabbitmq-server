-module(rabbit_channel_interceptor_new).

-export([init/1]).

-define(OLD_REGISTRY_CLASS, channel_interceptor).

init(Ch) ->
  VHost = rabbit_channel:get_vhost(Ch),
  fun (Method) ->
      io:format("intercept ~p ~n", [Method]),
      rabbit_channel_interceptor:intercept_method(Method, VHost)
  end.

-module(rabbit_channel_interceptor_new).

-export([init/1]).

init(Ch) ->
  VHost = rabbit_channel:get_vhost(Ch),
  fun (Method) ->
      rabbit_channel_interceptor:intercept_method(Method, VHost)
  end.

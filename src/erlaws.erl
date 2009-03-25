-module(erlaws). 

-behaviour(application). 

-export([start/0, start/2, stop/1]). 

start() ->
    application:start(sasl),
    crypto:start(),
    inets:start().

start(_Type, _Args) -> 
	erlaws:start().
	
stop(_State) -> 
    ok. 

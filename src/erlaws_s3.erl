%%%-------------------------------------------------------------------
%%% File    : erlaws_s3.erl
%%% Author  : Sascha Matzke <sascha.matzke@didolo.org>
%%% Description : Amazon S3 client library
%%%
%%% Created : 25 Dec 2007 by Sascha Matzke <sascha.matzke@didolo.org>
%%%-------------------------------------------------------------------

-module(erlaws_s3, [AWS_KEY, AWS_SEC_KEY, SECURE]).

%% API
-export([list_buckets/0, create_bucket/1, create_bucket/2, delete_bucket/1]).
-export([list_contents/1, list_contents/2, put_object/5, get_object/2]).
-export([info_object/2, delete_object/2]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").
-include("../include/erlaws.hrl").

%% macro definitions
-define( AWS_S3_HOST, "s3.amazonaws.com").
-define( NR_OF_RETRIES, 3).
-define( CALL_TIMEOUT, indefinite).
-define( S3_REQ_ID_HEADER, "x-amz-request-id").
-define( PREFIX_XPATH, "//CommonPrefixes/Prefix/text()").

%% Returns a list of all of the buckets owned by the authenticated sender 
%% of the request.
%%
%% Spec: list_buckets() -> 
%%       {ok, Buckets::[Name::string()]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
list_buckets() ->
    try genericRequest(get, "", "", "", [], "", <<>>) of
	{ok, Headers, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
	    TextNodes       = xmerl_xpath:string("//Bucket/Name/text()", XmlDoc),
	    BExtr = fun (#xmlText{value=T}) -> T end,
		RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, [BExtr(Node) || Node <- TextNodes], {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates a new bucket. Not every string is an acceptable bucket name. 
%% See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/UsingBucket.html
%% for information on bucket naming restrictions.
%% 
%% Spec: create_bucket(Bucket::string()) ->
%%       {ok, Bucket::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
create_bucket(Bucket) ->
    try genericRequest(put, Bucket, "", "", [], "", <<>>) of
	{ok, Headers, _Body} -> 
	    RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, Bucket, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates a new bucket with a location constraint (EU). 
%%
%% *** Be aware that Amazon applies a different pricing for EU buckets *** 
%%
%% Not every string is an acceptable bucket name. 
%% See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/UsingBucket.html
%% for information on bucket naming restrictions.
%% 
%% Spec: create_bucket(Bucket::string(), eu) ->
%%       {ok, Bucket::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
create_bucket(Bucket, eu) ->
    LCfg = <<"<CreateBucketConfiguration>
                  <LocationConstraint>EU</LocationConstraint>
             </CreateBucketConfiguration>">>,
    try genericRequest(put, Bucket, "", "", [], "", LCfg) of
	{ok, Headers, _Body} ->
		RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, Bucket, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes a bucket. 
%% 
%% Spec: delete_bucket(Bucket::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
delete_bucket(Bucket) ->
    try genericRequest(delete, Bucket, "", "", [], "", <<>>) of
	{ok, Headers, _Body} ->
	    RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Lists the contents of a bucket.
%%
%% Spec: list_contents(Bucket::string()) ->
%%       {ok, #s3_list_result{isTruncated::boolean(),
%%                         keys::[#s3_object_info{}],
%%                         prefix::[string()]}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
list_contents(Bucket) ->
    list_contents(Bucket, []).

%% Lists the contents of a bucket.
%%
%% Spec: list_contents(Bucket::string(), Options::[{atom(), 
%%                     (integer() | string())}]) ->
%%       {ok, #s3_list_result{isTruncated::boolean(),
%%                         keys::[#s3_object_info{}],
%%                         prefix::[string()]}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{prefix, string()}, {marker, string()},
%%	             {max_keys, integer()}, {delimiter, string()}]
%%
list_contents(Bucket, Options) when is_list(Options) ->
    QueryParameters = [makeParam(X) || X <- Options],
    try genericRequest(get, Bucket, "", QueryParameters, [], "", <<>>) of
	{ok, Headers, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
	    [Truncated| _Tail] = xmerl_xpath:string("//IsTruncated/text()", 
						    XmlDoc),
	    ContentNodes = xmerl_xpath:string("//Contents", XmlDoc),
	    KeyList = [extractObjectInfo(Node) || Node <- ContentNodes],
	    PrefixList = [Node#xmlText.value || 
			     Node <- xmerl_xpath:string(?PREFIX_XPATH, XmlDoc)],
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,			
	    {ok, #s3_list_result{isTruncated=case Truncated#xmlText.value of
					      "true" -> true;
					      _ -> false end, 
			      keys=KeyList, prefixes=PrefixList}, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Uploads data for key.
%%
%% Spec: put_object(Bucket::string(), Key::string(), Data::binary(),
%%                  ContentType::string(), 
%%                  Metadata::[{Key::string(), Value::string()}]) ->
%%       {ok, #s3_object_info(key=Key::string(), size=Size::integer())} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
put_object(Bucket, Key, Data, ContentType, Metadata) ->
    try genericRequest(put, Bucket, Key, [], Metadata, ContentType, Data) of
	{ok, Headers, _Body} -> 
	    RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, #s3_object_info{key=Key, size=size(Data)}, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
       
%% Retrieves the data associated with the given key.
%% 
%% Spec: get_object(Bucket::string(), Key::string()) ->
%%       {ok, Data::binary()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
get_object(Bucket, Key) ->
    try genericRequest(get, Bucket, Key, [], [], "", <<>>) of
	{ok, Headers, Body} -> 
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, Body, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Returns the metadata associated with the given key.
%%
%% Spec: info_object(Bucket::string(), Key::string()) ->
%%       {ok, [{Key::string(), Value::string()},...]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
info_object(Bucket, Key) ->
    try genericRequest(head, Bucket, Key, [], [], "", <<>>) of
	{ok, Headers, _Body} ->
	    io:format("Headers: ~p~n", [Headers]),
		MetadataList = [{string:substr(MKey, 12), Value} || {MKey, Value} <- Headers, string:str(MKey, "x-amz-meta") == 1],
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, MetadataList, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Delete the given key from bucket.
%% 
%% Spec: delete_object(Bucket::string(), Key::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
delete_object(Bucket, Key) ->
    try genericRequest(delete, Bucket, Key, [], [], "", <<>>) of
	{ok, Headers, _Body} ->
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

isAmzHeader( Header ) -> lists:prefix("x-amz-", Header).

aggregateValues ({K,V}, [{K,L}|T]) -> [{K,[V|L]}|T];
aggregateValues ({K,V}, L) -> [{K,[V]}|L].

collapse(L) ->
    AggrL = lists:foldl( fun aggregateValues/2, [], lists:keysort(1, L) ),
    lists:keymap( fun lists:sort/1, 2, lists:reverse(AggrL)).


mkHdr ({Key,Values}) ->
    Key ++ ":" ++ erlaws_util:mkEnumeration(Values,",").

canonicalizeAmzHeaders( Headers ) ->
    XAmzHeaders = [ {string:to_lower(Key),Value} || {Key,Value} <- Headers, 
						    isAmzHeader(Key) ],
    Strings = lists:map( 
		fun mkHdr/1, 
		collapse(XAmzHeaders)),
    erlaws_util:mkEnumeration( lists:map( fun (String) -> String ++ "\n" end, 
					  Strings), "").

canonicalizeResource ( "", "" ) -> "/";
canonicalizeResource ( Bucket, "" ) -> "/" ++ Bucket ++ "/";
canonicalizeResource ( "", Path) -> "/" ++ Path;
canonicalizeResource ( Bucket, Path ) -> "/" ++ Bucket ++ "/" ++ Path.

makeParam(X) ->
    case X of
	{_, []} -> {};
	{prefix, Prefix} -> 
	    {"prefix", Prefix};
	{marker, Marker} -> 
	    {"marker", Marker};
	{max_keys, MaxKeys} when is_integer(MaxKeys) -> 
	    {"max-keys", integer_to_list(MaxKeys)};
	{delimiter, Delimiter} -> 
	    {"delimiter", Delimiter};
	_ -> {}
    end.


buildHost("") ->
    ?AWS_S3_HOST;
buildHost(Bucket) ->
    Bucket ++ "." ++ ?AWS_S3_HOST.

buildProtocol() ->
	case SECURE of 
		true -> "https://";
		_ -> "http://" end.

buildUrl("", "", []) ->
    buildProtocol() ++ ?AWS_S3_HOST ++ "/";
buildUrl("", Path, []) ->
    buildProtocol() ++ ?AWS_S3_HOST ++ Path;
buildUrl(Bucket,Path,QueryParams) -> 
    buildProtocol() ++ Bucket ++ "." ++ ?AWS_S3_HOST ++ "/" ++ Path ++ 
	erlaws_util:queryParams(QueryParams).

buildContentHeaders( <<>>, _ ) -> [];
buildContentHeaders( Contents, ContentType ) -> 
    [{"Content-Length", integer_to_list(size(Contents))},
     {"Content-Type", ContentType}].

buildMetadataHeaders(Metadata) ->
    buildMetadataHeaders(Metadata, []).

buildMetadataHeaders([], Acc) ->
    Acc;
buildMetadataHeaders([{Key, Value}|Tail], Acc) ->
    buildMetadataHeaders(Tail, [{string:to_lower("x-amz-meta-"++Key), Value} 
				| Acc]).

buildContentMD5Header(ContentMD5) ->
    case ContentMD5 of
	"" -> [];
	_ -> [{"Content-MD5", ContentMD5}]
    end.

stringToSign ( Verb, ContentMD5, ContentType, Date, Bucket, Path, 
	       OriginalHeaders ) ->
    Parts = [ Verb, ContentMD5, ContentType, Date, 
	      canonicalizeAmzHeaders(OriginalHeaders)],
    erlaws_util:mkEnumeration( Parts, "\n") ++ 
	canonicalizeResource(Bucket, Path).

sign (Key,Data) ->
    %io:format("StringToSign:~n ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

genericRequest( Method, Bucket, Path, QueryParams, Metadata,
		ContentType, Body ) ->
    genericRequest( Method, Bucket, Path, QueryParams, Metadata,
		    ContentType, Body, ?NR_OF_RETRIES).

genericRequest( Method, Bucket, Path, QueryParams, Metadata, 
		ContentType, Body, NrOfRetries) ->
    Date = httpd_util:rfc1123_date(erlang:localtime()),
    MethodString = string:to_upper( atom_to_list(Method) ),
    Url = buildUrl(Bucket,Path,QueryParams),

    ContentMD5 = case Body of
		     <<>> -> "";
		     _ -> binary_to_list(base64:encode(erlang:md5(Body)))
		 end,
    
    Headers = buildContentHeaders( Body, ContentType ) ++
	buildMetadataHeaders(Metadata) ++ 
	buildContentMD5Header(ContentMD5),
    
    {AccessKey, SecretAccessKey } = {AWS_KEY, AWS_SEC_KEY},

    Signature = sign(SecretAccessKey,
		     stringToSign( MethodString, ContentMD5, ContentType, Date,
				   Bucket, Path, Headers )),
    
    FinalHeaders = [ {"Authorization","AWS " ++ AccessKey ++ ":" ++ Signature },
		     {"Host", buildHost(Bucket) },
		     {"Date", Date },
		     {"Expect", "Continue"}
		     | Headers ],

    Request = case Method of
 		  get -> { Url, FinalHeaders };
		  head -> { Url, FinalHeaders };
 		  put -> { Url, FinalHeaders, ContentType, Body };
 		  delete -> { Url, FinalHeaders }
 	      end,

    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],

    %%io:format("Request:~n ~p~n", [Request]),

    Reply = http:request( Method, Request, HttpOptions, Options ),
    
    %%     {ok, {Status, ReplyHeaders, RBody}} = Reply,
    %%     io:format("Response:~n ~p~n~p~n~p~n", [Status, ReplyHeaders, 
    %% 					   binary_to_list(RBody)]),
    
    case Reply of
 	{ok, {{_HttpVersion, Code, _ReasonPhrase}, ResponseHeaders, 
	      ResponseBody }} when Code=:=200; Code=:=204 -> 
 	    {ok, ResponseHeaders, ResponseBody};
	
	{ok, {{_HttpVersion, Code, ReasonPhrase}, ResponseHeaders, 
	      _ResponseBody }} when Code=:=500, NrOfRetries > 0 ->
	    throw ({error, "500", ReasonPhrase, 
		    proplists:get_value(?S3_REQ_ID_HEADER, ResponseHeaders)});
	
	{ok, {{_HttpVersion, Code, _ReasonPhrase}, _ResponseHeaders, 
	      _ResponseBody }} when Code=:=500 ->
	    timer:sleep((?NR_OF_RETRIES-NrOfRetries)*500),
	    genericRequest(Method, Bucket, Path, QueryParams, 
			   Metadata, ContentType, Body, NrOfRetries-1);
	
 	{ok, {{_HttpVersion, _HttpCode, _ReasonPhrase}, ResponseHeaders, 
	      ResponseBody }} ->
 	    throw ( mkErr(ResponseBody, ResponseHeaders) )
    end.

mkErr (Xml, Headers) ->
    {XmlDoc, _Rest} = xmerl_scan:string( binary_to_list(Xml) ),
    [#xmlText{value=ErrorCode}|_] = 
	xmerl_xpath:string("/Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}|_] = 
	xmerl_xpath:string("/Error/Message/text()", XmlDoc),
    {error, {ErrorCode, ErrorMessage, 
	     proplists:get_value(?S3_REQ_ID_HEADER, Headers)}}.

extractObjectInfo (Node) -> 
    [Key|_] = xmerl_xpath:string("./Key/text()", Node),
    [ETag|_] = xmerl_xpath:string("./ETag/text()", Node),
    [LastModified|_] = xmerl_xpath:string("./LastModified/text()", Node),
    [Size|_] = xmerl_xpath:string("./Size/text()", Node),
    #s3_object_info{key=Key#xmlText.value, lastmodified=LastModified#xmlText.value,
		 etag=ETag#xmlText.value, size=Size#xmlText.value}.




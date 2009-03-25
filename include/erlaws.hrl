%%
%% erlaws record definitions
%%

-record( s3_list_result, {isTruncated=false, keys=[], prefixes=[]}).
-record( s3_object_info, {key, lastmodified, etag, size} ).

-record( sqs_queue, {nrOfMessages, visibilityTimeout}).
-record( sqs_message, {messageId, receiptHandle="", contentMD5, body}).
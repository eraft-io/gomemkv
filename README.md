# gomemkv
A Distributed memory kv service

## Supported command list


command | Time complexity| 
--- | --- | 
set | O(1) | 
get | O(1) | 
del | O(1) | 
append | O(1) | 
strlen | O(1) | 
setrange | O(1) | 
lpush | O(1) | 
lpop | O(N) | 
rpush | O(1) | 
rpop | O(N) | 
lrange | O(S + N) | 
llen | O(1) | 
sadd | O(1) | 
smemebers | O(N) | 
scard | O(1) | 
srandmember | O(1) | 
srem | O(N) | 

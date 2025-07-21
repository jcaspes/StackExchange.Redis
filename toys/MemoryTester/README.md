# MemoryTester

This tool allow to test stackexchange.redis library under high memory load conditions.

# How it work

- Connect to redis with a ConnectionMultiplexer.
- Create 200 threads (simulate a website with 200 users connections).
- During thread start, generate a random string with random size up to 0x24000 bytes and tagged with start ">>>!" and end "!<<<" for simple data check.
- Set in redis a hset where the key is the thread + a root. this hset contains a "thread"" field with thread id and a "value"" field with the generated random string.
- Then we enter an infinite loop on each thread
  - Get the hset with a hgetall command.
  - Check if the returned hashset is not null (we found it). **This should not happen without caller error.**
  - If the command is successful, check if the value field contains valid data (start and end tags).
  - Check if thread id in the hset is the same as the current thread id => log a message if not, **this should not happen, it say we received data from an other thread.**
  - If the content is not valid, log an error message if asked in commandline. **This should not happen wiothout error notifiying caller**

### Usage:

MemoryTester.exe [checkContent] [log] [maxSize=123456789] 
- checkContent: Optional, if present will check if the content of the value field is valid (start and end tags).
- log: Optional, if present will log unique stacks traces to a log file + all console output.
- maxSize: Optional, if present will set the maximum size of the random string to generate (default is 0x24000 bytes).

All logs start with thread id

#### Simple usage:
Will only check that the hgetall returned the asked hashset and if data is valid (simple check of size and start / end tags)
```
MemoryTester.exe
```
Ouput sample (say that key is from an other thread, and if data is valid (but from an other thread)):
```
1 : Starting the tests (press any key to stop)
87 : Error in key, expected:Th87hT, actual:Th71hT, value is a valid but from an other query: True
126 : Error in key, expected:Th126hT, actual:Th56hT, value is a valid but from an other query: True
Exiting the test loop.
```

#### Data check usage:
Will check if received content (value field) contain valid data (simple check of size and start /end tags)
```
MemoryTester.exe checkContent
```
Ouput sample:
```
1 : Starting the tests (press any key to stop)

67 : Error in key, expected:Th67hT, actual:Th54hT, value is a valid but from an other query: True
67 : >>>!KVWVPW8ACCD.....2UFKD8HVMRK12!<<<
67 : Error in value lenght in thread 'Th67hT', expected:57399, actual:100094, value is a valid but from an other query: True
67 : >>>!R2T3UJFRG3M.....6EQQHPN14CEG2!<<<
218 : Error in key, expected:Th218hT, actual:Th58hT, value is a valid but from an other query: True
218 : Error in value lenght in thread 'Th218hT', expected:34098, actual:118465, value is a valid but from an other query: True
```

Program will also output stats about exceptions type and stack hash
```
1 : -------------------------------
1 : Exception: OutOfMemoryException_1457523110, Count: 2517
1 : Exception: OutOfMemoryException_1134558920, Count: 451
1 : Exception: OutOfMemoryException_880579577, Count: 159
1 : Exception: OutOfMemoryException_977447609, Count: 162
1 : Exception: RedisTimeoutException_1886063157, Count: 188
1 : Exception: OutOfMemoryException_1329206684, Count: 3
1 : Exception: RedisConnectionException_757602046, Count: 17
1 : Exception: RedisConnectionException_NoStackTrace, Count: 292
1 : Exception: RedisConnectionException_977447609, Count: 1140
1 : Exception: RedisConnectionException_1315139194, Count: 60
1 : Exception: RedisConnectionException_-2035699568, Count: 5
1 : Exception: RedisConnectionException_-706126008, Count: 31
1 : Exception: RedisConnectionException_1234414936, Count: 9
```

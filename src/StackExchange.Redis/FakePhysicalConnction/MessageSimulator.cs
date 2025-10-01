using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Pipelines.Sockets.Unofficial.Arenas;

namespace StackExchange.Redis
{
    internal class MessageSimulator
    {
        internal Message? message;
        internal Header? header;
        internal List<RedisValue> values = new List<RedisValue>();
        internal RedisKey key;
        internal bool simulated = false;
        private static readonly ConcurrentDictionary<string, string> _hashStorage = new ConcurrentDictionary<string, string>();
        private string identifier = string.Empty;
        private int connectionIndex;

        internal ReadStatus SimulateCompleteMessage(IPhysicalConnection connection, string identifier, int connectionIndex)
        {
            this.identifier = identifier;
            this.connectionIndex = connectionIndex;
            simulated = true;
            RawResult result = GenerateResult();
            message!.ComputeResult(connection, result);
            // Method should be async becasue of caller thread will call Monitor.Wait on the message source
            Task.Run(() =>
            {
                Thread.Sleep(1);
                message.Complete();
            });
            return message.ResultBoxIsAsync ? ReadStatus.CompletePendingMessageAsync : ReadStatus.CompletePendingMessageSync;
        }

        private RawResult GenerateResult()
        {
            return header!.command switch
            {
                RedisCommand.AUTH
                    or RedisCommand.READWRITE
                    or RedisCommand.CLIENT => CreateSimpleStringResult("OK"),
                RedisCommand.CONFIG => CreateCONFIGStringResult(),
                RedisCommand.SENTINEL => CreateSimpleStringResult("ERR unknown command `sentinel`, with args beginning with: `MASTER`"),
                RedisCommand.INFO => CreateINFOStringResult(),
                RedisCommand.CLUSTER => CreateSimpleStringResult("ERR This instance has cluster support disabled"),
                RedisCommand.GET => CreateSimpleGETResult(),
                RedisCommand.ECHO => CreateEchoResult(values[0]!),
                RedisCommand.SUBSCRIBE => CreateSimpleStringResult("__Booksleeve_MasterChanged"),
                RedisCommand.PING => CreateSimpleStringResult("PONG"),
                RedisCommand.SCAN => CreateScanResult(),
                RedisCommand.HMSET => CreateHMSETResult(),
                RedisCommand.EXPIRE => CreateSimpleIntegerResult(1),
                RedisCommand.HGETALL => CreateHGetHALLResult(),
                _ => throw new DebugException(identifier, connectionIndex, $"No simulation for command {header.command}"),
            };
        }

        private RawResult CreateHMSETResult()
        {
            if (values.Count % 2 != 0)
            {
                throw new DebugException(identifier, connectionIndex, "HMSET requires an even number of values");
            }

            for (int i = 0; i < values.Count; i += 2)
            {
                string dictKey = GetDictKeyForHset(values[i]!);
                _hashStorage.TryAdd(dictKey, values[i + 1]!);
            }

            return CreateSimpleStringResult("OK");
        }

        private string GetDictKeyForHset(string value) => $"{key}|{value}";

        private RawResult CreateSimpleIntegerResult(long value)
        {
            ReadOnlySequence<byte> readOnlySequenceBytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(value.ToString()));
            RawResult result = new RawResult(ResultType.Integer, readOnlySequenceBytes, RawResult.ResultFlags.None);
            return result;
        }

        private RawResult CreateScanResult(string cursor, List<string> elements)
        {
            Sequence<RawResult> seq;
            {
                var array = new RawResult[2];
                {
                    array[0] = CreateSimpleStringResult(cursor);
                }
                {
                    var itemsSeq = new Sequence<RawResult>(new RawResult[0]);
                    array[1] = new RawResult(ResultType.Array, itemsSeq, RawResult.ResultFlags.None);
                }
                seq = new Sequence<RawResult>(array);
            }

            RawResult result = new RawResult(ResultType.Array, seq, RawResult.ResultFlags.None);
            return result;
        }

        private RawResult CreateScanResult()
        {
            if (values[2] == "MemoryTester:*")
            {
                return CreateScanResult("0", new List<string> { "MemoryTester:1" });
            }
            throw new DebugException(identifier, connectionIndex, $"No simulation for SCAN with args {string.Join(", ", values)}");
        }

        private RawResult CreateEchoResult(RedisValue value)
        {
            ReadOnlySequence<byte> readOnlySequenceBytes = new ReadOnlySequence<byte>((byte[])value.DirectObject!);
            RawResult result = new RawResult(ResultType.BulkString, readOnlySequenceBytes, RawResult.ResultFlags.None);
            return result;
        }

        private RawResult CreateSimpleStringResult(string wantedResultString)
        {
            ReadOnlySequence<byte> readOnlySequenceBytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(wantedResultString));
            RawResult result = new RawResult(ResultType.SimpleString, readOnlySequenceBytes, RawResult.ResultFlags.None);
            return result;
        }

        private RawResult CreateSimpleGETResult()
        {
            return (string?)key switch
            {
                "__Booksleeve_TieBreak" => CreateSimpleStringResult("null"),
                _ => throw new DebugException(identifier, connectionIndex, $"No simulation for GET {key}"),
            };
        }

        private RawResult CreateCONFIGStringResult()
        {
            if (values[0] == "GET")
            {
                string wantedResultString;
                if (values[1] == "slave-read-only")
                {
                    wantedResultString = $"{values[0]}\r\nyes";
                }
                else if (values[1] == "databases")
                {
                    wantedResultString = $"{values[0]}\r\n16";
                }
                else
                {
                    throw new DebugException(identifier, connectionIndex, $"No simulation for CONFIG GET {values[1]}");
                }
                return CreateSimpleStringResult(wantedResultString);
            }
            else
            {
                throw new DebugException(identifier, connectionIndex, $"No simulation for CONFIG {values[0]}");
            }
        }

        private RawResult CreateINFOStringResult()
        {
            string wantedResultString;
            if (values[0] == "replication")
            {
                wantedResultString = @"# Replication
role:master
connected_slaves:0
master_replid:c8f1471760f305bf504b1fb416b85525ae4c5f1d
master_replid2:0000000000000000000000000000000000000000
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:0";
            }
            else if (values[0] == "server")
            {
                wantedResultString = @"# Server
redis_version:6.0.14
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:7786e16076e24d08
redis_mode:standalone
os:CYGWIN_NT-10.0-20348 3.4.6-1.x86_64 x86_64
arch_bits:64
multiplexing_api:select
atomicvar_api:atomic-builtin
gcc_version:11.3.0
process_id:4183
run_id:17eda6f5483ce193d9c1c67335ef6c7f397d8107
tcp_port:7379
uptime_in_seconds:265399
uptime_in_days:3
hz:10
configured_hz:10
lru_clock:14065145
executable:/Redis/redis-server
config_file:/Redis/D:\APPS\Redis\redis.windows-service2.conf
io_threads_active:0";
            }
            else
            {
                throw new DebugException(identifier, connectionIndex, $"No simulation for INFO {values[0]}");
            }
            return CreateSimpleStringResult(wantedResultString);
        }

        private RawResult CreateHGetHALLResult()
        {
            // Simulate HGETALL by returning all key-value pairs for the given hash key
            var results = new List<RawResult>();
            string prefix = $"{key}|";
            foreach (var kvp in _hashStorage)
            {
                if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    // Extract the field name from the composite key
                    string field = kvp.Key.Substring(prefix.Length);
                    results.Add(CreateSimpleStringResult(field));
                    results.Add(CreateSimpleStringResult(kvp.Value));
                }
            }
            var seq = new Sequence<RawResult>(results.ToArray());
            return new RawResult(ResultType.Array, seq, RawResult.ResultFlags.None);
        }
    }
}

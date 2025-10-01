using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace StackExchange.Redis
{
    internal class MessageSimulator
    {
        internal Message? message;
        internal Header? header;
        internal List<RedisValue> values = new List<RedisValue>();
        internal RedisKey key;
        internal bool simulated = false;

        internal ReadStatus SimulateCompleteMessage(IPhysicalConnection connection)
        {
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
                RedisCommand.ECHO => CreateSimpleStringResult(values[0]!),
                RedisCommand.SUBSCRIBE => CreateSimpleStringResult("__Booksleeve_MasterChanged"),
                _ => throw new DebugException($"No simulation for command {header.command}"),
            };
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
                _ => throw new DebugException($"No simulation for GET {key}"),
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
                    throw new DebugException($"No simulation for CONFIG GET {values[1]}");
                }
                return CreateSimpleStringResult(wantedResultString);
            }
            else
            {
                throw new DebugException($"No simulation for CONFIG {values[0]}");
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
                throw new DebugException($"No simulation for INFO {values[0]}");
            }
            return CreateSimpleStringResult(wantedResultString);
        }
    }
}

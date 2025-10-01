using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace StackExchange.Redis
{
    internal class FakePhysicalConnection : IDisposable, IPhysicalConnection
    {
        private readonly PhysicalBridge _physicalBridge;
        private int _currentDataBase = 0;
        private RedisProtocol _redisProtocol;
        private WriteStatus _writeStatus = WriteStatus.NA;
        private ReadStatus _readStatus = ReadStatus.NA;
        private static int connectionIndex = 0;
        private int currentconnectionIndex;
        private readonly ConcurrentDictionary<string, MessageSimulator> _simulatedConnection = new ConcurrentDictionary<string, MessageSimulator>();

        private string Identifier
        {
            get
            {
                string identifier = Thread.CurrentThread.Name!;
                if (string.IsNullOrEmpty(identifier))
                {
                    identifier = $"Th{Thread.CurrentThread.ManagedThreadId}";
                }
                return identifier;
            }
        }

        public Socket? VolatileSocket => throw new DebugException();

        public FakePhysicalConnection(PhysicalBridge physicalBridge) => _physicalBridge = physicalBridge;

        public PhysicalBridge? BridgeCouldBeNull => _physicalBridge;

        public long LastReadSecondsAgo => throw new DebugException();

        public long LastWriteSecondsAgo => throw new DebugException();

        public RedisProtocol? Protocol => _redisProtocol;

        public long SubscriptionCount { get => throw new DebugException(); set => throw new DebugException(); }
        public bool TransactionActive { get; set; }
        public long? ConnectionId { get => throw new DebugException(); set => throw new DebugException(); }

        public bool HasOutputPipe => throw new DebugException();

        public bool MultiDatabasesOverride { get => throw new DebugException(); set => throw new DebugException(); }

        public byte[]? ChannelPrefix => throw new DebugException();

        public async Task BeginConnectAsync(ILogger? log)
        {
            currentconnectionIndex = Interlocked.Increment(ref connectionIndex);
            Trace($"Connecting... {currentconnectionIndex}");
            await BridgeCouldBeNull!.OnConnectedAsync(this, log).ForAwait();
            Trace("Connecting...END");
        }

        public void Dispose() => throw new DebugException();

        public void EnqueueInsideWriteLock(Message next)
        {
            Trace($"EnqueueInsideWriteLock: {next.CommandString}");

            MessageSimulator im = new MessageSimulator() { message = next };
            if (next.Command == RedisCommand.READWRITE)
            {
                // im.SimulateCompleteMessage(this);
                return;
            }
            _simulatedConnection.AddOrUpdate(Identifier, im, (k, v) =>
            {
                if (!v.simulated)
                {
                    throw new DebugException("Message not simulated ! can not update");
                }
                return im;
            });
        }

        public Task FlushAsync()
        {
            Trace("FlushAsync()");
            if (_simulatedConnection.TryGetValue(Identifier, out MessageSimulator? im))
            {
                return Task.CompletedTask;
            }
            else
            {
                throw new DebugException("No message queued for this thread");
            }
        }

        public ValueTask<WriteResult> FlushAsync(bool throwOnFailure, CancellationToken cancellationToken = default)
        {
            Trace("FlushAsyncValueTask");
            if (_simulatedConnection.TryGetValue(Identifier, out MessageSimulator? im))
            {
                _readStatus = im.SimulateCompleteMessage(this);
                return new ValueTask<WriteResult>(WriteResult.Success);
            }
            else
            {
                throw new DebugException("No message queued for this thread");
            }
        }

        public WriteResult FlushSync(bool throwOnFailure, int millisecondsTimeout) => throw new DebugException();
        public void GetBytes(out long sent, out long received)
        {
            sent = received = 0;
            Trace($"GetBytes s{sent}r{received}");
        }
        public void GetCounters(ConnectionCounters counters) => throw new DebugException();
        public void GetHeadMessages(out Message? now, out Message? next) => throw new DebugException();
        public Message? GetReadModeCommand(bool isPrimaryOnly)
        {
            Trace($"GetReadModeCommand");
            return null; // PhysicalConnectionHelpers.ReusableReadWriteCommand;
        }
        public ReadStatus GetReadStatus()
        {
            Trace($"GetReadStatus {_readStatus}");
            return _readStatus;
        }

        public Message? GetSelectDatabaseCommand(int targetDatabase, Message message)
        {
            Trace($"GetSelectDatabaseCommand");
            _currentDataBase = targetDatabase; // test do not change database, fake database is the same has wanted
            return null;
        }
        public int GetSentAwaitingResponseCount() => throw new DebugException();
        public ConnectionStatus GetStatus()
        {
            Trace($"GetStatus {Environment.StackTrace}");
            return new ConnectionStatus()
            {
                BytesAvailableOnSocket = -1,
                BytesInReadPipe = -1,
                BytesInWritePipe = -1,
                ReadStatus = ReadStatus.Reconfigure,
                WriteStatus = WriteStatus.Flushed,
                BytesLastResult = -1,
                BytesInBuffer = -1,
            };
        }
        public void GetStormLog(StringBuilder sb) => throw new DebugException();
        public WriteStatus GetWriteStatus() => _writeStatus;
        public bool HasPendingCallerFacingItems() => throw new DebugException();

        public int OnBridgeHeartbeat() => throw new DebugException();
        public void OnInternalError(Exception exception, [CallerMemberName] string? origin = null) => throw new DebugException();
        public void RecordConnectionFailed(ConnectionFailureType failureType, Exception? innerException = null, [CallerMemberName] string? origin = null, bool isInitialConnect = false, IDuplexPipe? connectingPipe = null)
        {
            // OKfailed
            Trace($"RecordConnectionFailed: {failureType}");
        }
        public void RecordQuit() => throw new DebugException();

        public bool IsIdle()
        {
            Trace("IsIdle");
            return _writeStatus == WriteStatus.Idle;
        }
        public void SetIdle()
        {
            Trace("SetIdle");
            SetWriteStatus(WriteStatus.Idle);
        }

        public void SetProtocol(RedisProtocol value)
        {
            Trace($"SetProtocol {value}");
            _redisProtocol = value;
        }
        public void SetUnknownDatabase() => throw new DebugException();

        public void SetWriteStatus(WriteStatus status)
        {
            Trace($"SetWriteStatus {status}");
            _writeStatus = status;
        }
        public void SetWriting() => SetWriteStatus(WriteStatus.Writing);
        public void Shutdown() => Trace($"Shutdown...");
        public void SimulateConnectionFailure(SimulatedFailureType failureType) => throw new DebugException();
        public void Trace(string message)
        {
            Console.WriteLine($"Trace(Th:{Identifier}:Cx{currentconnectionIndex}): {message}");
        }
        public void UpdateLastWriteTime() => throw new DebugException();
        public void Write(in RedisChannel channel)
        {
            // don't care about channels
            string sValue = Encoding.UTF8.GetString(channel.Value!, 0, channel.Value!.Length);
            Trace($"WriteChannel:{channel.PublishCommand}|{sValue}");

            // only on config broadcast to channel during start so only simulate that
            ReadOnlySequence<byte> readOnlySequenceBytes = new ReadOnlySequence<byte>(channel.Value);
            RawResult result = new RawResult(ResultType.SimpleString, readOnlySequenceBytes, RawResult.ResultFlags.None);
            BridgeCouldBeNull?.Multiplexer.OnMessage(channel, channel, result.AsRedisValue());
        }
        public void Write(in RedisKey key)
        {
            Trace($"WriteKey: {key}");
            if (_simulatedConnection.TryGetValue(Identifier, out MessageSimulator? im))
            {
                im.key = key;
            }
            else
            {
                throw new DebugException("No message queued for this thread");
            }
        }

        public void WriteBulkString(in RedisValue value)
        {
            Trace($"WriteBulkString: {value}");
            if (_simulatedConnection.TryGetValue(Identifier, out MessageSimulator? im))
            {
                im.values.Add(value);
            }
            else
            {
                throw new DebugException("No message queued for this thread");
            }
        }
        public void WriteBulkString(ReadOnlySpan<byte> value) => throw new DebugException();

        public void WriteHeader(RedisCommand command, int arguments, CommandBytes commandBytes = default)
        {
            Trace($"WriteHeader: {command}|{arguments}|{commandBytes}");
            if (_simulatedConnection.TryGetValue(Identifier, out MessageSimulator? im))
            {
                im.header = new Header() { command = command, arguments = arguments, commandBytes = commandBytes };
            }
            else
            {
                throw new DebugException("No message queued for this thread");
            }
        }

        public void WriteRaw(ReadOnlySpan<byte> chk) => throw new DebugException();
        public void WriteSha1AsHex(byte[] value) => throw new DebugException();
    }
}

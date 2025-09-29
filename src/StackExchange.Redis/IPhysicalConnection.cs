using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace StackExchange.Redis
{
    internal interface IPhysicalConnection : IDisposable
    {
        Socket? VolatileSocket { get; }

        byte[]? ChannelPrefix { get; }

        bool MultiDatabasesOverride { get; set; }

        void Trace(string message);

        PhysicalBridge? BridgeCouldBeNull { get; }

        long LastReadSecondsAgo { get; }
        long LastWriteSecondsAgo { get; }
        RedisProtocol? Protocol { get; }
        long SubscriptionCount { get; set; }

        bool TransactionActive { get; set; }

        Task FlushAsync();
        ConnectionStatus GetStatus();
        void RecordConnectionFailed(ConnectionFailureType failureType, Exception? innerException = null, [CallerMemberName] string? origin = null, bool isInitialConnect = false, IDuplexPipe? connectingPipe = null);
        void SetWriteStatus(WriteStatus status);
        void UpdateLastWriteTime();
        void OnInternalError(Exception exception, [CallerMemberName] string? origin = null);
        void WriteHeader(RedisCommand command, int arguments, CommandBytes commandBytes = default);
        void WriteRaw(ReadOnlySpan<byte> chk);
        WriteStatus GetWriteStatus();
        ReadStatus GetReadStatus();
        void GetBytes(out long sent, out long received);
        void GetHeadMessages(out Message? now, out Message? next);
        void WriteBulkString(in RedisValue value);
        void WriteBulkString(ReadOnlySpan<byte> value);
        void Write(in RedisChannel channel);
        void Write(in RedisKey key);
        Task BeginConnectAsync(ILogger? log);

        long? ConnectionId { get; set; }

        void WriteSha1AsHex(byte[] value);
        void Shutdown();

        bool HasOutputPipe { get; }

        WriteResult FlushSync(bool throwOnFailure, int millisecondsTimeout);

        void SetIdle();
        bool IsIdle();

        bool HasPendingCallerFacingItems();

        ValueTask<WriteResult> FlushAsync(bool throwOnFailure, CancellationToken cancellationToken = default);

        void GetCounters(ConnectionCounters counters);
        void GetStormLog(StringBuilder sb);

        int GetSentAwaitingResponseCount();

        int OnBridgeHeartbeat();

        void SetWriting();

        Message? GetSelectDatabaseCommand(int targetDatabase, Message message);
        void EnqueueInsideWriteLock(Message next);

        void RecordQuit();

        Message? GetReadModeCommand(bool isPrimaryOnly);

        void SetUnknownDatabase();

        void SimulateConnectionFailure(SimulatedFailureType failureType);

        void SetProtocol(RedisProtocol value);
    }
}

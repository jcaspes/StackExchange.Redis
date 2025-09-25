using System;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace StackExchange.Redis
{
    internal class FakePhysicalConnection : IDisposable, IPhysicalConnection
    {
        public PhysicalBridge? BridgeCouldBeNull => throw new NotImplementedException();

        public long LastReadSecondsAgo => throw new NotImplementedException();

        public long LastWriteSecondsAgo => throw new NotImplementedException();

        public RedisProtocol? Protocol => throw new NotImplementedException();

        public long SubscriptionCount { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool TransactionActive { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public long? ConnectionId { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public bool HasOutputPipe => throw new NotImplementedException();

        public bool MultiDatabasesOverride { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public byte[]? ChannelPrefix => throw new NotImplementedException();

        public Task BeginConnectAsync(ILogger? log) => throw new NotImplementedException();
        public void Dispose() => throw new NotImplementedException();
        public void EnqueueInsideWriteLock(Message next) => throw new NotImplementedException();
        public Task FlushAsync() => throw new NotImplementedException();
        public ValueTask<WriteResult> FlushAsync(bool throwOnFailure, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public WriteResult FlushSync(bool throwOnFailure, int millisecondsTimeout) => throw new NotImplementedException();
        public void GetBytes(out long sent, out long received) => throw new NotImplementedException();
        public void GetCounters(ConnectionCounters counters) => throw new NotImplementedException();
        public void GetHeadMessages(out Message? now, out Message? next) => throw new NotImplementedException();
        public Message? GetReadModeCommand(bool isPrimaryOnly) => throw new NotImplementedException();
        public ReadStatus GetReadStatus() => throw new NotImplementedException();
        public Message? GetSelectDatabaseCommand(int targetDatabase, Message message) => throw new NotImplementedException();
        public int GetSentAwaitingResponseCount() => throw new NotImplementedException();
        public ConnectionStatus GetStatus() => throw new NotImplementedException();
        public void GetStormLog(StringBuilder sb) => throw new NotImplementedException();
        public WriteStatus GetWriteStatus() => throw new NotImplementedException();
        public bool HasPendingCallerFacingItems() => throw new NotImplementedException();
        public bool IsIdle() => throw new NotImplementedException();
        public int OnBridgeHeartbeat() => throw new NotImplementedException();
        public void OnInternalError(Exception exception, [CallerMemberName] string? origin = null) => throw new NotImplementedException();
        public void RecordConnectionFailed(ConnectionFailureType failureType, Exception? innerException = null, [CallerMemberName] string? origin = null, bool isInitialConnect = false, IDuplexPipe? connectingPipe = null) => throw new NotImplementedException();
        public void RecordQuit() => throw new NotImplementedException();
        public void SetIdle() => throw new NotImplementedException();
        public void SetProtocol(RedisProtocol value) => throw new NotImplementedException();
        public void SetUnknownDatabase() => throw new NotImplementedException();
        public void SetWriteStatus(WriteStatus flushed) => throw new NotImplementedException();
        public void SetWriting() => throw new NotImplementedException();
        public void Shutdown() => throw new NotImplementedException();
        public void SimulateConnectionFailure(SimulatedFailureType failureType) => throw new NotImplementedException();
        public void Trace(string message) => throw new NotImplementedException();
        public void UpdateLastWriteTime() => throw new NotImplementedException();
        public void Write(in RedisChannel channel) => throw new NotImplementedException();
        public void Write(in RedisKey key) => throw new NotImplementedException();
        public void WriteBulkString(in RedisValue value) => throw new NotImplementedException();
        public void WriteBulkString(ReadOnlySpan<byte> value) => throw new NotImplementedException();
        public void WriteHeader(RedisCommand command, int arguments, CommandBytes commandBytes = default) => throw new NotImplementedException();
        public void WriteRaw(ReadOnlySpan<byte> chk) => throw new NotImplementedException();
        public void WriteSha1AsHex(byte[] value) => throw new NotImplementedException();
    }
}

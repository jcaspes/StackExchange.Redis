using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pipelines.Sockets.Unofficial;
using Pipelines.Sockets.Unofficial.Arenas;
using static StackExchange.Redis.Message;

namespace StackExchange.Redis
{
    internal partial class PhysicalConnection : IDisposable, IPhysicalConnection
    {
        byte[]? IPhysicalConnection.ChannelPrefix => _ChannelPrefix;

        private readonly byte[]? _ChannelPrefix;

        private static int totalCount;

        private readonly ConnectionType connectionType;

        // things sent to this physical, but not yet received
        private readonly Queue<Message> _writtenAwaitingResponse = new Queue<Message>();

        private Message? _awaitingToken;

        private readonly string _physicalName;

        private volatile int currentDatabase = 0;

        private ReadMode currentReadMode = ReadMode.NotSpecified;

        private int failureReported;

        private int clientSentQuit;

        private int lastWriteTickCount, lastReadTickCount, lastBeatTickCount;

        private long bytesLastResult;
        private long bytesInBuffer;
        public long? ConnectionId { get; set; }

        public void GetBytes(out long sent, out long received)
        {
            if (_ioPipe is IMeasuredDuplexPipe sc)
            {
                sent = sc.TotalBytesSent;
                received = sc.TotalBytesReceived;
            }
            else
            {
                sent = received = -1;
            }
        }

        /// <summary>
        /// Nullable because during simulation of failure, we'll null out.
        /// ...but in those cases, we'll accept any null ref in a race - it's fine.
        /// </summary>
        private IDuplexPipe? _ioPipe;
        public bool HasOutputPipe => _ioPipe?.Output != null;

        private Socket? _socket;
        internal Socket? VolatileSocket => Volatile.Read(ref _socket);

        public PhysicalConnection(PhysicalBridge bridge)
        {
            lastWriteTickCount = lastReadTickCount = Environment.TickCount;
            lastBeatTickCount = 0;
            connectionType = bridge.ConnectionType;
            _bridge = new WeakReference(bridge);
            _ChannelPrefix = bridge.Multiplexer.RawConfig.ChannelPrefix;
            if (_ChannelPrefix?.Length == 0) _ChannelPrefix = null; // null tests are easier than null+empty
            var endpoint = bridge.ServerEndPoint.EndPoint;
            _physicalName = connectionType + "#" + Interlocked.Increment(ref totalCount) + "@" + Format.ToString(endpoint);

            OnCreateEcho();
        }

        // *definitely* multi-database; this can help identify some unusual config scenarios
        public bool MultiDatabasesOverride { get; set; } // switch to flags-enum if more needed later

        public async Task BeginConnectAsync(ILogger? log)
        {
            var bridge = BridgeCouldBeNull;
            var endpoint = bridge?.ServerEndPoint?.EndPoint;
            if (bridge == null || endpoint == null)
            {
                log?.LogErrorNoEndpoint(new ArgumentNullException(nameof(endpoint)));
                return;
            }

            Trace("Connecting...");
            var tunnel = bridge.Multiplexer.RawConfig.Tunnel;
            var connectTo = endpoint;
            if (tunnel is not null)
            {
                connectTo = await tunnel.GetSocketConnectEndpointAsync(endpoint, CancellationToken.None).ForAwait();
            }
            if (connectTo is not null)
            {
                _socket = SocketManager.CreateSocket(connectTo);
            }

            if (_socket is not null)
            {
                bridge.Multiplexer.RawConfig.BeforeSocketConnect?.Invoke(endpoint, bridge.ConnectionType, _socket);
                if (tunnel is not null)
                {
                    // same functionality as part of a tunnel
                    await tunnel.BeforeSocketConnectAsync(endpoint, bridge.ConnectionType, _socket, CancellationToken.None).ForAwait();
                }
            }
            bridge.Multiplexer.OnConnecting(endpoint, bridge.ConnectionType);
            log?.LogInformationBeginConnectAsync(new(endpoint));

            CancellationTokenSource? timeoutSource = null;
            try
            {
                using (var args = connectTo is null ? null : new SocketAwaitableEventArgs
                {
                    RemoteEndPoint = connectTo,
                })
                {
                    var x = VolatileSocket;
                    if (x == null)
                    {
                        args?.Abort();
                    }
                    else if (args is not null && x.ConnectAsync(args))
                    {
                        // asynchronous operation is pending
                        timeoutSource = ConfigureTimeout(args, bridge.Multiplexer.RawConfig.ConnectTimeout);
                    }
                    else
                    {
                        // completed synchronously
                        args?.Complete();
                    }

                    // Complete connection
                    try
                    {
                        // If we're told to ignore connect, abort here
                        if (BridgeCouldBeNull?.Multiplexer?.IgnoreConnect ?? false) return;

                        if (args is not null)
                        {
                            await args; // wait for the connect to complete or fail (will throw)
                        }
                        if (timeoutSource != null)
                        {
                            timeoutSource.Cancel();
                            timeoutSource.Dispose();
                        }

                        x = VolatileSocket;
                        if (x == null && args is not null)
                        {
                            ConnectionMultiplexer.TraceWithoutContext("Socket was already aborted");
                        }
                        else if (await ConnectedAsync(x, log, bridge.Multiplexer.SocketManager!).ForAwait())
                        {
                            log?.LogInformationStartingRead(new(endpoint));
                            try
                            {
                                StartReading();
                                // Normal return
                            }
                            catch (Exception ex)
                            {
                                ConnectionMultiplexer.TraceWithoutContext(ex.Message);
                                Shutdown();
                            }
                        }
                        else
                        {
                            ConnectionMultiplexer.TraceWithoutContext("Aborting socket");
                            Shutdown();
                        }
                    }
                    catch (ObjectDisposedException ex)
                    {
                        log?.LogErrorSocketShutdown(ex, new(endpoint));
                        try { RecordConnectionFailed(ConnectionFailureType.UnableToConnect, isInitialConnect: true); }
                        catch (Exception inner)
                        {
                            ConnectionMultiplexer.TraceWithoutContext(inner.Message);
                        }
                    }
                    catch (Exception outer)
                    {
                        ConnectionMultiplexer.TraceWithoutContext(outer.Message);
                        try { RecordConnectionFailed(ConnectionFailureType.UnableToConnect, isInitialConnect: true); }
                        catch (Exception inner)
                        {
                            ConnectionMultiplexer.TraceWithoutContext(inner.Message);
                        }
                    }
                }
            }
            catch (NotImplementedException ex) when (endpoint is not IPEndPoint)
            {
                throw new InvalidOperationException("BeginConnect failed with NotImplementedException; consider using IP endpoints, or enable ResolveDns in the configuration", ex);
            }
            finally
            {
                if (timeoutSource != null) try { timeoutSource.Dispose(); } catch { }
            }
        }

        private static CancellationTokenSource ConfigureTimeout(SocketAwaitableEventArgs args, int timeoutMilliseconds)
        {
            var cts = new CancellationTokenSource();
            var timeout = Task.Delay(timeoutMilliseconds, cts.Token);
            timeout.ContinueWith(
                (_, state) =>
                {
                    try
                    {
                        var a = (SocketAwaitableEventArgs)state!;
                        a.Abort(SocketError.TimedOut);
                        Socket.CancelConnectAsync(a);
                    }
                    catch { }
                },
                args);
            return cts;
        }

        private enum ReadMode : byte
        {
            NotSpecified,
            ReadOnly,
            ReadWrite,
        }

        private readonly WeakReference _bridge;
        public PhysicalBridge? BridgeCouldBeNull => (PhysicalBridge?)_bridge.Target;

        public long LastReadSecondsAgo => unchecked(Environment.TickCount - Thread.VolatileRead(ref lastReadTickCount)) / 1000;
        public long LastWriteSecondsAgo => unchecked(Environment.TickCount - Thread.VolatileRead(ref lastWriteTickCount)) / 1000;

        private bool IncludeDetailInExceptions => BridgeCouldBeNull?.Multiplexer.RawConfig.IncludeDetailInExceptions ?? false;

        [Conditional("VERBOSE")]
        private void TraceInternal(string message) => BridgeCouldBeNull?.Multiplexer?.Trace(message, ToString());

        public void Trace(string message) => TraceInternal(message);

        public long SubscriptionCount { get; set; }

        public bool TransactionActive { get; set; }

        private RedisProtocol _protocol; // note starts at **zero**, not RESP2
        public RedisProtocol? Protocol => _protocol == 0 ? null : _protocol;

        public void SetProtocol(RedisProtocol value)
        {
            _protocol = value;
            BridgeCouldBeNull?.SetProtocol(value);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:Do not dispose objects multiple times", Justification = "Trust me yo")]
        public void Shutdown()
        {
            var ioPipe = Interlocked.Exchange(ref _ioPipe, null); // compare to the critical read
            var socket = Interlocked.Exchange(ref _socket, null);

            if (ioPipe != null)
            {
                Trace("Disconnecting...");
                try { BridgeCouldBeNull?.OnDisconnected(ConnectionFailureType.ConnectionDisposed, this, out _, out _); } catch { }
                try { ioPipe.Input?.CancelPendingRead(); } catch { }
                try { ioPipe.Input?.Complete(); } catch { }
                try { ioPipe.Output?.CancelPendingFlush(); } catch { }
                try { ioPipe.Output?.Complete(); } catch { }
                try { using (ioPipe as IDisposable) { } } catch { }
            }

            if (socket != null)
            {
                try { socket.Shutdown(SocketShutdown.Both); } catch { }
                try { socket.Close(); } catch { }
                try { socket.Dispose(); } catch { }
            }
        }

        public void Dispose()
        {
            bool markDisposed = VolatileSocket != null;
            Shutdown();
            if (markDisposed)
            {
                Trace("Disconnected");
                RecordConnectionFailed(ConnectionFailureType.ConnectionDisposed);
            }
            OnCloseEcho();
            _arena.Dispose();
            _reusableFlushSyncTokenSource?.Dispose();
            GC.SuppressFinalize(this);
        }

        private async Task AwaitedFlush(ValueTask<FlushResult> flush)
        {
            await flush.ForAwait();
            SetWriteStatus(WriteStatus.Flushed);
            UpdateLastWriteTime();
        }
        public void UpdateLastWriteTime() => Interlocked.Exchange(ref lastWriteTickCount, Environment.TickCount);

        public Task FlushAsync()
        {
            var tmp = _ioPipe?.Output;
            if (tmp != null)
            {
                SetWriteStatus(WriteStatus.Flushing);
                var flush = tmp.FlushAsync();
                if (!flush.IsCompletedSuccessfully)
                {
                    return AwaitedFlush(flush);
                }
                SetWriteStatus(WriteStatus.Flushed);
                UpdateLastWriteTime();
            }
            return Task.CompletedTask;
        }

        public void SimulateConnectionFailure(SimulatedFailureType failureType)
        {
            var raiseFailed = false;
            if (connectionType == ConnectionType.Interactive)
            {
                if (failureType.HasFlag(SimulatedFailureType.InteractiveInbound))
                {
                    _ioPipe?.Input.Complete(new Exception("Simulating interactive input failure"));
                    raiseFailed = true;
                }
                if (failureType.HasFlag(SimulatedFailureType.InteractiveOutbound))
                {
                    _ioPipe?.Output.Complete(new Exception("Simulating interactive output failure"));
                    raiseFailed = true;
                }
            }
            else if (connectionType == ConnectionType.Subscription)
            {
                if (failureType.HasFlag(SimulatedFailureType.SubscriptionInbound))
                {
                    _ioPipe?.Input.Complete(new Exception("Simulating subscription input failure"));
                    raiseFailed = true;
                }
                if (failureType.HasFlag(SimulatedFailureType.SubscriptionOutbound))
                {
                    _ioPipe?.Output.Complete(new Exception("Simulating subscription output failure"));
                    raiseFailed = true;
                }
            }
            if (raiseFailed)
            {
                RecordConnectionFailed(ConnectionFailureType.SocketFailure);
            }
        }

        public void RecordConnectionFailed(
            ConnectionFailureType failureType,
            Exception? innerException = null,
            [CallerMemberName] string? origin = null,
            bool isInitialConnect = false,
            IDuplexPipe? connectingPipe = null)
        {
            bool weAskedForThis;
            Exception? outerException = innerException;
            IdentifyFailureType(innerException, ref failureType);
            var bridge = BridgeCouldBeNull;
            Message? nextMessage;

            if (_ioPipe != null || isInitialConnect) // if *we* didn't burn the pipe: flag it
            {
                if (failureType == ConnectionFailureType.InternalFailure && innerException is not null)
                {
                    OnInternalError(innerException, origin);
                }

                // stop anything new coming in...
                bridge?.Trace("Failed: " + failureType);
                ConnectionStatus connStatus = ConnectionStatus.Default;
                PhysicalBridge.State oldState = PhysicalBridge.State.Disconnected;
                bool isCurrent = false;
                bridge?.OnDisconnected(failureType, this, out isCurrent, out oldState);
                if (oldState == PhysicalBridge.State.ConnectedEstablished)
                {
                    try
                    {
                        connStatus = GetStatus();
                    }
                    catch { /* best effort only */ }
                }

                if (isCurrent && Interlocked.CompareExchange(ref failureReported, 1, 0) == 0)
                {
                    int now = Environment.TickCount, lastRead = Thread.VolatileRead(ref lastReadTickCount), lastWrite = Thread.VolatileRead(ref lastWriteTickCount),
                        lastBeat = Thread.VolatileRead(ref lastBeatTickCount);

                    int unansweredWriteTime = 0;
                    lock (_writtenAwaitingResponse)
                    {
                        // find oldest message awaiting a response
                        if (_writtenAwaitingResponse.TryPeek(out nextMessage))
                        {
                            unansweredWriteTime = nextMessage.GetWriteTime();
                        }
                    }

                    var exMessage = new StringBuilder(failureType.ToString());

                    // If the reason for the shutdown was we asked for the socket to die, don't log it as an error (only informational)
                    weAskedForThis = Thread.VolatileRead(ref clientSentQuit) != 0;

                    var pipe = connectingPipe ?? _ioPipe;
                    if (pipe is SocketConnection sc)
                    {
                        exMessage.Append(" (").Append(sc.ShutdownKind);
                        if (sc.SocketError != SocketError.Success)
                        {
                            exMessage.Append('/').Append(sc.SocketError);
                        }
                        if (sc.BytesRead == 0) exMessage.Append(", 0-read");
                        if (sc.BytesSent == 0) exMessage.Append(", 0-sent");
                        exMessage.Append(", last-recv: ").Append(sc.LastReceived).Append(')');
                    }
                    else if (pipe is IMeasuredDuplexPipe mdp)
                    {
                        long sent = mdp.TotalBytesSent, recd = mdp.TotalBytesReceived;

                        if (sent == 0) { exMessage.Append(recd == 0 ? " (0-read, 0-sent)" : " (0-sent)"); }
                        else if (recd == 0) { exMessage.Append(" (0-read)"); }
                    }

                    var data = new List<Tuple<string, string?>>();
                    void AddData(string lk, string sk, string? v)
                    {
                        if (lk != null) data.Add(Tuple.Create(lk, v));
                        if (sk != null) exMessage.Append(", ").Append(sk).Append(": ").Append(v);
                    }

                    if (IncludeDetailInExceptions)
                    {
                        if (bridge != null)
                        {
                            exMessage.Append(" on ").Append(Format.ToString(bridge.ServerEndPoint?.EndPoint)).Append('/').Append(connectionType)
                                .Append(", ").Append(_writeStatus).Append('/').Append(_readStatus)
                                .Append(", last: ").Append(bridge.LastCommand);

                            data.Add(Tuple.Create<string, string?>("FailureType", failureType.ToString()));
                            data.Add(Tuple.Create<string, string?>("EndPoint", Format.ToString(bridge.ServerEndPoint?.EndPoint)));

                            AddData("Origin", "origin", origin);
                            // add("Input-Buffer", "input-buffer", _ioPipe.Input);
                            AddData("Outstanding-Responses", "outstanding", GetSentAwaitingResponseCount().ToString());
                            AddData("Last-Read", "last-read", (unchecked(now - lastRead) / 1000) + "s ago");
                            AddData("Last-Write", "last-write", (unchecked(now - lastWrite) / 1000) + "s ago");
                            if (unansweredWriteTime != 0) AddData("Unanswered-Write", "unanswered-write", (unchecked(now - unansweredWriteTime) / 1000) + "s ago");
                            AddData("Keep-Alive", "keep-alive", bridge.ServerEndPoint?.WriteEverySeconds + "s");
                            AddData("Previous-Physical-State", "state", oldState.ToString());
                            AddData("Manager", "mgr", bridge.Multiplexer.SocketManager?.GetState());
                            if (connStatus.BytesAvailableOnSocket >= 0) AddData("Inbound-Bytes", "in", connStatus.BytesAvailableOnSocket.ToString());
                            if (connStatus.BytesInReadPipe >= 0) AddData("Inbound-Pipe-Bytes", "in-pipe", connStatus.BytesInReadPipe.ToString());
                            if (connStatus.BytesInWritePipe >= 0) AddData("Outbound-Pipe-Bytes", "out-pipe", connStatus.BytesInWritePipe.ToString());

                            AddData("Last-Heartbeat", "last-heartbeat", (lastBeat == 0 ? "never" : ((unchecked(now - lastBeat) / 1000) + "s ago")) + (bridge.IsBeating ? " (mid-beat)" : ""));
                            var mbeat = bridge.Multiplexer.LastHeartbeatSecondsAgo;
                            if (mbeat >= 0)
                            {
                                AddData("Last-Multiplexer-Heartbeat", "last-mbeat", mbeat + "s ago");
                            }
                            AddData("Last-Global-Heartbeat", "global", ConnectionMultiplexer.LastGlobalHeartbeatSecondsAgo + "s ago");
                        }
                    }

                    AddData("Version", "v", Utils.GetLibVersion());

                    outerException = new RedisConnectionException(failureType, exMessage.ToString(), innerException);

                    foreach (var kv in data)
                    {
                        outerException.Data["Redis-" + kv.Item1] = kv.Item2;
                    }

                    bridge?.OnConnectionFailed(this, failureType, outerException, wasRequested: weAskedForThis);
                }
            }
            // clean up (note: avoid holding the lock when we complete things, even if this means taking
            // the lock multiple times; this is fine here - we shouldn't be fighting anyone, and we're already toast)
            lock (_writtenAwaitingResponse)
            {
                bridge?.Trace(_writtenAwaitingResponse.Count != 0, "Failing outstanding messages: " + _writtenAwaitingResponse.Count);
            }

            var ex = innerException is RedisException ? innerException : outerException;

            nextMessage = Interlocked.Exchange(ref _awaitingToken, null);
            if (nextMessage is not null)
            {
                RecordMessageFailed(nextMessage, ex, origin, bridge);
            }

            while (TryDequeueLocked(_writtenAwaitingResponse, out nextMessage))
            {
                RecordMessageFailed(nextMessage, ex, origin, bridge);
            }

            // burn the socket
            Shutdown();

            static bool TryDequeueLocked(Queue<Message> queue, [NotNullWhen(true)] out Message? message)
            {
                lock (queue)
                {
                    return queue.TryDequeue(out message);
                }
            }
        }

        private void RecordMessageFailed(Message next, Exception? ex, string? origin, PhysicalBridge? bridge)
        {
            if (next.Command == RedisCommand.QUIT && next.TrySetResult(true))
            {
                // fine, death of a socket is close enough
                next.Complete();
            }
            else
            {
                if (bridge != null)
                {
                    bridge.Trace("Failing: " + next);
                    bridge.Multiplexer?.OnMessageFaulted(next, ex, origin);
                }
                next.SetExceptionAndComplete(ex!, bridge);
            }
        }

        public bool IsIdle() => _writeStatus == WriteStatus.Idle;
        public void SetIdle() => SetWriteStatus(WriteStatus.Idle);
        public void SetWriting() => SetWriteStatus(WriteStatus.Writing);
        public void SetWriteStatus(WriteStatus status) => _writeStatus = status;

        private volatile WriteStatus _writeStatus;

        public WriteStatus GetWriteStatus() => _writeStatus;

        /// <summary>Returns a string that represents the current object.</summary>
        /// <returns>A string that represents the current object.</returns>
        public override string ToString() => $"{_physicalName} ({_writeStatus})";

        internal static void IdentifyFailureType(Exception? exception, ref ConnectionFailureType failureType)
        {
            if (exception != null && failureType == ConnectionFailureType.InternalFailure)
            {
                if (exception is AggregateException)
                {
                    exception = exception.InnerException ?? exception;
                }

                failureType = exception switch
                {
                    AuthenticationException => ConnectionFailureType.AuthenticationFailure,
                    EndOfStreamException or ObjectDisposedException => ConnectionFailureType.SocketClosed,
                    SocketException or IOException => ConnectionFailureType.SocketFailure,
                    _ => failureType,
                };
            }
        }

        public void EnqueueInsideWriteLock(Message next)
        {
            var multiplexer = BridgeCouldBeNull?.Multiplexer;
            if (multiplexer is null)
            {
                // multiplexer already collected? then we're almost certainly doomed;
                // we can still process it to avoid making things worse/more complex,
                // but: we can't reliably assume this works, so: shout now!
                next.Cancel();
                next.Complete();
            }

            bool wasEmpty;
            lock (_writtenAwaitingResponse)
            {
                wasEmpty = _writtenAwaitingResponse.Count == 0;
                _writtenAwaitingResponse.Enqueue(next);
            }
            if (wasEmpty)
            {
                // it is important to do this *after* adding, so that we can't
                // get into a thread-race where the heartbeat checks too fast;
                // the fact that we're accessing Multiplexer down here means that
                // we're rooting it ourselves via the stack, so we don't need
                // to worry about it being collected until at least after this
                multiplexer?.Root();
            }
        }

        public void GetCounters(ConnectionCounters counters)
        {
            lock (_writtenAwaitingResponse)
            {
                counters.SentItemsAwaitingResponse = _writtenAwaitingResponse.Count;
            }
            counters.Subscriptions = SubscriptionCount;
        }

        public Message? GetReadModeCommand(bool isPrimaryOnly)
        {
            if (BridgeCouldBeNull?.ServerEndPoint?.RequiresReadMode == true)
            {
                ReadMode requiredReadMode = isPrimaryOnly ? ReadMode.ReadWrite : ReadMode.ReadOnly;
                if (requiredReadMode != currentReadMode)
                {
                    currentReadMode = requiredReadMode;
                    switch (requiredReadMode)
                    {
                        case ReadMode.ReadOnly: return PhysicalConnectionHelpers.ReusableReadOnlyCommand;
                        case ReadMode.ReadWrite: return PhysicalConnectionHelpers.ReusableReadWriteCommand;
                    }
                }
            }
            else if (currentReadMode == ReadMode.ReadOnly)
            {
                // we don't need it (because we're not a cluster, or not a replica),
                // but we are in read-only mode; switch to read-write
                currentReadMode = ReadMode.ReadWrite;
                return PhysicalConnectionHelpers.ReusableReadWriteCommand;
            }
            return null;
        }

        public Message? GetSelectDatabaseCommand(int targetDatabase, Message message)
        {
            if (targetDatabase < 0 || targetDatabase == currentDatabase)
            {
                return null;
            }

            if (BridgeCouldBeNull?.ServerEndPoint is not ServerEndPoint serverEndpoint)
            {
                return null;
            }
            int available = serverEndpoint.Databases;

            // Only db0 is available on cluster/twemproxy/envoyproxy
            if (!serverEndpoint.SupportsDatabases)
            {
                if (targetDatabase != 0)
                {
                    // We should never see this, since the API doesn't allow it; thus not too worried about ExceptionFactory
                    throw new RedisCommandException("Multiple databases are not supported on this server; cannot switch to database: " + targetDatabase);
                }
                return null;
            }

            if (message.Command == RedisCommand.SELECT)
            {
                // This could come from an EVAL/EVALSHA inside a transaction, for example; we'll accept it
                BridgeCouldBeNull?.Trace("Switching database: " + targetDatabase);
                currentDatabase = targetDatabase;
                return null;
            }

            if (TransactionActive)
            {
                // Should never see this, since the API doesn't allow it, thus not too worried about ExceptionFactory
                throw new RedisCommandException("Multiple databases inside a transaction are not currently supported: " + targetDatabase);
            }

            // We positively know it is out of range
            if (available != 0 && targetDatabase >= available)
            {
                throw ExceptionFactory.DatabaseOutfRange(IncludeDetailInExceptions, targetDatabase, message, serverEndpoint);
            }
            BridgeCouldBeNull?.Trace("Switching database: " + targetDatabase);
            currentDatabase = targetDatabase;
            return PhysicalConnectionHelpers.GetSelectDatabaseCommand(targetDatabase);
        }

        public int GetSentAwaitingResponseCount()
        {
            lock (_writtenAwaitingResponse)
            {
                return _writtenAwaitingResponse.Count;
            }
        }

        public void GetStormLog(StringBuilder sb)
        {
            lock (_writtenAwaitingResponse)
            {
                if (_writtenAwaitingResponse.Count == 0) return;
                sb.Append("Sent, awaiting response from server: ").Append(_writtenAwaitingResponse.Count).AppendLine();
                int total = 0;
                foreach (var item in _writtenAwaitingResponse)
                {
                    if (++total >= 500) break;
                    item.AppendStormLog(sb);
                    sb.AppendLine();
                }
            }
        }

        /// <summary>
        /// Runs on every heartbeat for a bridge, timing out any commands that are overdue and returning an integer of how many we timed out.
        /// </summary>
        /// <returns>How many commands were overdue and threw timeout exceptions.</returns>
        public int OnBridgeHeartbeat()
        {
            var result = 0;
            var now = Environment.TickCount;
            Interlocked.Exchange(ref lastBeatTickCount, now);

            lock (_writtenAwaitingResponse)
            {
                if (_writtenAwaitingResponse.Count != 0 && BridgeCouldBeNull is PhysicalBridge bridge)
                {
                    var server = bridge.ServerEndPoint;
                    var multiplexer = bridge.Multiplexer;
                    var timeout = multiplexer.AsyncTimeoutMilliseconds;
                    foreach (var msg in _writtenAwaitingResponse)
                    {
                        // We only handle async timeouts here, synchronous timeouts are handled upstream.
                        // Those sync timeouts happen in ConnectionMultiplexer.ExecuteSyncImpl() via Monitor.Wait.
                        if (msg.HasTimedOut(now, timeout, out var elapsed))
                        {
                            if (msg.ResultBoxIsAsync)
                            {
                                bool haveDeltas = msg.TryGetPhysicalState(out _, out _, out long sentDelta, out var receivedDelta) && sentDelta >= 0 && receivedDelta >= 0;
                                var baseErrorMessage = haveDeltas
                                    ? $"Timeout awaiting response (outbound={sentDelta >> 10}KiB, inbound={receivedDelta >> 10}KiB, {elapsed}ms elapsed, timeout is {timeout}ms)"
                                    : $"Timeout awaiting response ({elapsed}ms elapsed, timeout is {timeout}ms)";
                                var timeoutEx = ExceptionFactory.Timeout(multiplexer, baseErrorMessage, msg, server);
                                multiplexer.OnMessageFaulted(msg, timeoutEx);
                                msg.SetExceptionAndComplete(timeoutEx, bridge); // tell the message that it is doomed
                                multiplexer.OnAsyncTimeout();
                                result++;
                            }
                        }
                        else
                        {
                            // This is a head-of-line queue, which means the first thing we hit that *hasn't* timed out means no more will timeout
                            // and we can stop looping and release the lock early.
                            break;
                        }
                        // Note: it is important that we **do not** remove the message unless we're tearing down the socket; that
                        // would disrupt the chain for MatchResult; we just preemptively abort the message from the caller's
                        // perspective, and set a flag on the message so we don't keep doing it
                    }
                }
            }
            return result;
        }

        public void OnInternalError(Exception exception, [CallerMemberName] string? origin = null)
        {
            if (BridgeCouldBeNull is PhysicalBridge bridge)
            {
                bridge.Multiplexer.OnInternalError(exception, bridge.ServerEndPoint.EndPoint, connectionType, origin);
            }
        }

        public void SetUnknownDatabase()
        {
            // forces next db-specific command to issue a select
            currentDatabase = -1;
        }

        public void Write(in RedisKey key)
        {
            var val = key.KeyValue;
            if (val is string s)
            {
               PhysicalConnectionHelpers.WriteUnifiedPrefixedString(_ioPipe?.Output, key.KeyPrefix, s);
            }
            else
            {
                PhysicalConnectionHelpers.WriteUnifiedPrefixedBlob(_ioPipe?.Output, key.KeyPrefix, (byte[]?)val);
            }
        }

        public void Write(in RedisChannel channel)
            => PhysicalConnectionHelpers.WriteUnifiedPrefixedBlob(_ioPipe?.Output, _ChannelPrefix, channel.Value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteBulkString(in RedisValue value)
            => WriteBulkString(value, _ioPipe?.Output);
        public static void WriteBulkString(in RedisValue value, PipeWriter? maybeNullWriter)
        {
            if (maybeNullWriter is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            switch (value.Type)
            {
                case RedisValue.StorageType.Null:
                    PhysicalConnectionHelpers.WriteUnifiedBlob(writer, (byte[]?)null);
                    break;
                case RedisValue.StorageType.Int64:
                    PhysicalConnectionHelpers.WriteUnifiedInt64(writer, value.OverlappedValueInt64);
                    break;
                case RedisValue.StorageType.UInt64:
                    PhysicalConnectionHelpers.WriteUnifiedUInt64(writer, value.OverlappedValueUInt64);
                    break;
                case RedisValue.StorageType.Double:
                    PhysicalConnectionHelpers.WriteUnifiedDouble(writer, value.OverlappedValueDouble);
                    break;
                case RedisValue.StorageType.String:
                    PhysicalConnectionHelpers.WriteUnifiedPrefixedString(writer, null, (string?)value);
                    break;
                case RedisValue.StorageType.Raw:
                    PhysicalConnectionHelpers.WriteUnifiedSpan(writer, ((ReadOnlyMemory<byte>)value).Span);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected {value.Type} value: '{value}'");
            }
        }

        public void WriteBulkString(ReadOnlySpan<byte> value)
        {
            if (_ioPipe?.Output is { } writer)
            {
                PhysicalConnectionHelpers.WriteUnifiedSpan(writer, value);
            }
        }

        public void WriteHeader(RedisCommand command, int arguments, CommandBytes commandBytes = default)
        {
            if (_ioPipe?.Output is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            var bridge = BridgeCouldBeNull ?? throw new ObjectDisposedException(ToString());

            if (command == RedisCommand.UNKNOWN)
            {
                // using >= here because we will be adding 1 for the command itself (which is an arg for the purposes of the multi-bulk protocol)
                if (arguments >= PhysicalConnectionHelpers.REDIS_MAX_ARGS) throw ExceptionFactory.TooManyArgs(commandBytes.ToString(), arguments);
            }
            else
            {
                // using >= here because we will be adding 1 for the command itself (which is an arg for the purposes of the multi-bulk protocol)
                if (arguments >= PhysicalConnectionHelpers.REDIS_MAX_ARGS) throw ExceptionFactory.TooManyArgs(command.ToString(), arguments);

                // for everything that isn't custom commands: ask the muxer for the actual bytes
                commandBytes = bridge.Multiplexer.CommandMap.GetBytes(command);
            }

            // in theory we should never see this; CheckMessage dealt with "regular" messages, and
            // ExecuteMessage should have dealt with everything else
            if (commandBytes.IsEmpty) throw ExceptionFactory.CommandDisabled(command);

            // *{argCount}\r\n      = 3 + MaxInt32TextLen
            // ${cmd-len}\r\n       = 3 + MaxInt32TextLen
            // {cmd}\r\n            = 2 + commandBytes.Length
            var span = writer.GetSpan(commandBytes.Length + 8 + Format.MaxInt32TextLen + Format.MaxInt32TextLen);
            span[0] = (byte)'*';

            int offset = PhysicalConnectionHelpers.WriteRaw(span, arguments + 1, offset: 1);

            offset = PhysicalConnectionHelpers.AppendToSpanCommand(span, commandBytes, offset: offset);

            writer.Advance(offset);
        }

        public void WriteRaw(ReadOnlySpan<byte> bytes) => _ioPipe?.Output?.Write(bytes);

        public void RecordQuit()
        {
            // don't blame redis if we fired the first shot
            Thread.VolatileWrite(ref clientSentQuit, 1);
            (_ioPipe as SocketConnection)?.TrySetProtocolShutdown(PipeShutdownKind.ProtocolExitClient);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "DEBUG uses instance data")]
        private async ValueTask<WriteResult> FlushAsync_Awaited(IPhysicalConnection connection, ValueTask<FlushResult> flush, bool throwOnFailure)
        {
            try
            {
                await flush.ForAwait();
                connection.SetWriteStatus(WriteStatus.Flushed);
                connection.UpdateLastWriteTime();
                return WriteResult.Success;
            }
            catch (ConnectionResetException ex) when (!throwOnFailure)
            {
                connection.RecordConnectionFailed(ConnectionFailureType.SocketClosed, ex);
                return WriteResult.WriteFailure;
            }
        }

        private CancellationTokenSource? _reusableFlushSyncTokenSource;
        [Obsolete("this is an anti-pattern; work to reduce reliance on this is in progress")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0062:Make local function 'static'", Justification = "DEBUG uses instance data")]
        public WriteResult FlushSync(bool throwOnFailure, int millisecondsTimeout)
        {
            var cts = _reusableFlushSyncTokenSource ??= new CancellationTokenSource();
            var flush = FlushAsync(throwOnFailure, cts.Token);
            if (!flush.IsCompletedSuccessfully)
            {
                // only schedule cancellation if it doesn't complete synchronously; at this point, it is doomed
                _reusableFlushSyncTokenSource = null;
                cts.CancelAfter(TimeSpan.FromMilliseconds(millisecondsTimeout));
                try
                {
                    // here lies the evil
                    flush.AsTask().Wait();
                }
                catch (AggregateException ex) when (ex.InnerExceptions.Any(e => e is TaskCanceledException))
                {
                    ThrowTimeout();
                }
                finally
                {
                    cts.Dispose();
                }
            }
            return flush.Result;

            void ThrowTimeout()
            {
                throw new TimeoutException("timeout while synchronously flushing");
            }
        }
        public ValueTask<WriteResult> FlushAsync(bool throwOnFailure, CancellationToken cancellationToken = default)
        {
            var tmp = _ioPipe?.Output;
            if (tmp == null) return new ValueTask<WriteResult>(WriteResult.NoConnectionAvailable);
            try
            {
                SetWriteStatus(WriteStatus.Flushing);
                var flush = tmp.FlushAsync(cancellationToken);
                if (!flush.IsCompletedSuccessfully) return FlushAsync_Awaited(this, flush, throwOnFailure);
                SetWriteStatus(WriteStatus.Flushed);
                UpdateLastWriteTime();
                return new ValueTask<WriteResult>(WriteResult.Success);
            }
            catch (ConnectionResetException ex) when (!throwOnFailure)
            {
                RecordConnectionFailed(ConnectionFailureType.SocketClosed, ex);
                return new ValueTask<WriteResult>(WriteResult.WriteFailure);
            }
        }
        public void WriteSha1AsHex(byte[] value)
        {
            if (_ioPipe?.Output is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            if (value == null)
            {
                writer.Write(PhysicalConnectionHelpers.NullBulkString.Span);
            }
            else if (value.Length == ResultProcessor.ScriptLoadProcessor.Sha1HashLength)
            {
                // $40\r\n              = 5
                // {40 bytes}\r\n       = 42
                var span = writer.GetSpan(47);
                span[0] = (byte)'$';
                span[1] = (byte)'4';
                span[2] = (byte)'0';
                span[3] = (byte)'\r';
                span[4] = (byte)'\n';

                int offset = 5;
                for (int i = 0; i < value.Length; i++)
                {
                    var b = value[i];
                    span[offset++] = PhysicalConnectionHelpers.ToHexNibble(b >> 4);
                    span[offset++] = PhysicalConnectionHelpers.ToHexNibble(b & 15);
                }
                span[offset++] = (byte)'\r';
                span[offset++] = (byte)'\n';

                writer.Advance(offset);
            }
            else
            {
                throw new InvalidOperationException("Invalid SHA1 length: " + value.Length);
            }
        }

        public ConnectionStatus GetStatus()
        {
            if (_ioPipe is SocketConnection conn)
            {
                var counters = conn.GetCounters();
                return new ConnectionStatus()
                {
                    MessagesSentAwaitingResponse = GetSentAwaitingResponseCount(),
                    BytesAvailableOnSocket = counters.BytesAvailableOnSocket,
                    BytesInReadPipe = counters.BytesWaitingToBeRead,
                    BytesInWritePipe = counters.BytesWaitingToBeSent,
                    ReadStatus = _readStatus,
                    WriteStatus = _writeStatus,
                    BytesLastResult = bytesLastResult,
                    BytesInBuffer = bytesInBuffer,
                };
            }

            // Fall back to bytes waiting on the socket if we can
            int fallbackBytesAvailable;
            try
            {
                fallbackBytesAvailable = VolatileSocket?.Available ?? -1;
            }
            catch
            {
                // If this fails, we're likely in a race disposal situation and do not want to blow sky high here.
                fallbackBytesAvailable = -1;
            }

            return new ConnectionStatus()
            {
                BytesAvailableOnSocket = fallbackBytesAvailable,
                BytesInReadPipe = -1,
                BytesInWritePipe = -1,
                ReadStatus = _readStatus,
                WriteStatus = _writeStatus,
                BytesLastResult = bytesLastResult,
                BytesInBuffer = bytesInBuffer,
            };
        }

        internal async ValueTask<bool> ConnectedAsync(Socket? socket, ILogger? log, SocketManager manager)
        {
            var bridge = BridgeCouldBeNull;
            if (bridge == null) return false;

            IDuplexPipe? pipe = null;
            try
            {
                // disallow connection in some cases
                OnDebugAbort();

                // the order is important here:
                // non-TLS: [Socket]<==[SocketConnection:IDuplexPipe]
                // TLS:     [Socket]<==[NetworkStream]<==[SslStream]<==[StreamConnection:IDuplexPipe]
                var config = bridge.Multiplexer.RawConfig;

                var tunnel = config.Tunnel;
                Stream? stream = null;
                if (tunnel is not null)
                {
                    stream = await tunnel.BeforeAuthenticateAsync(bridge.ServerEndPoint.EndPoint, bridge.ConnectionType, socket, CancellationToken.None).ForAwait();
                }

                if (config.Ssl)
                {
                    log?.LogInformationConfiguringTLS();
                    var host = config.SslHost;
                    if (host.IsNullOrWhiteSpace())
                    {
                        host = Format.ToStringHostOnly(bridge.ServerEndPoint.EndPoint);
                    }

                    stream ??= new NetworkStream(socket ?? throw new InvalidOperationException("No socket or stream available - possibly a tunnel error"));
                    var ssl = new SslStream(
                        innerStream: stream,
                        leaveInnerStreamOpen: false,
                        userCertificateValidationCallback: config.CertificateValidationCallback ?? PhysicalConnectionHelpers.GetAmbientIssuerCertificateCallback(),
                        userCertificateSelectionCallback: config.CertificateSelectionCallback ?? PhysicalConnectionHelpers.GetAmbientClientCertificateCallback(),
                        encryptionPolicy: EncryptionPolicy.RequireEncryption);
                    try
                    {
                        try
                        {
#if NETCOREAPP3_1_OR_GREATER
                            var configOptions = config.SslClientAuthenticationOptions?.Invoke(host);
                            if (configOptions is not null)
                            {
                                await ssl.AuthenticateAsClientAsync(configOptions).ForAwait();
                            }
                            else
                            {
                                await ssl.AuthenticateAsClientAsync(host, config.SslProtocols, config.CheckCertificateRevocation).ForAwait();
                            }
#else
                            await ssl.AuthenticateAsClientAsync(host, config.SslProtocols, config.CheckCertificateRevocation).ForAwait();
#endif
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex.Message);
                            bridge.Multiplexer.SetAuthSuspect(ex);
                            bridge.Multiplexer.Logger?.LogErrorConnectionIssue(ex, ex.Message);
                            throw;
                        }
                        log?.LogInformationTLSConnectionEstablished(ssl.SslProtocol);
                    }
                    catch (AuthenticationException authexception)
                    {
                        RecordConnectionFailed(ConnectionFailureType.AuthenticationFailure, authexception, isInitialConnect: true);
                        bridge.Multiplexer.Trace("Encryption failure");
                        return false;
                    }
                    stream = ssl;
                }

                if (stream is not null)
                {
                    pipe = StreamConnection.GetDuplex(stream, manager.SendPipeOptions, manager.ReceivePipeOptions, name: bridge.Name);
                }
                else
                {
                    pipe = SocketConnection.Create(socket, manager.SendPipeOptions, manager.ReceivePipeOptions, name: bridge.Name);
                }
                OnWrapForLogging(ref pipe, _physicalName, manager);

                _ioPipe = pipe;

                log?.LogInformationConnected(bridge.Name);

                await bridge.OnConnectedAsync(this, log).ForAwait();
                return true;
            }
            catch (Exception ex)
            {
                RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex, isInitialConnect: true, connectingPipe: pipe); // includes a bridge.OnDisconnected
                bridge.Multiplexer.Trace("Could not connect: " + ex.Message, ToString());
                return false;
            }
        }

        private void MatchResult(in RawResult result)
        {
            // check to see if it could be an out-of-band pubsub message
            if ((connectionType == ConnectionType.Subscription && result.Resp2TypeArray == ResultType.Array) || result.Resp3Type == ResultType.Push)
            {
                var muxer = BridgeCouldBeNull?.Multiplexer;
                if (muxer == null) return;

                // out of band message does not match to a queued message
                var items = result.GetItems();
                if (items.Length >= 3 && (items[0].IsEqual(PhysicalConnectionHelpers.message) || items[0].IsEqual(PhysicalConnectionHelpers.smessage)))
                {
                    _readStatus = items[0].IsEqual(PhysicalConnectionHelpers.message) ? ReadStatus.PubSubMessage : ReadStatus.PubSubSMessage;

                    // special-case the configuration change broadcasts (we don't keep that in the usual pub/sub registry)
                    var configChanged = muxer.ConfigurationChangedChannel;
                    if (configChanged != null && items[1].IsEqual(configChanged))
                    {
                        EndPoint? blame = null;
                        try
                        {
                            if (!items[2].IsEqual(CommonReplies.wildcard))
                            {
                                // We don't want to fail here, just trying to identify
                                _ = Format.TryParseEndPoint(items[2].GetString(), out blame);
                            }
                        }
                        catch { /* no biggie */ }
                        Trace("Configuration changed: " + Format.ToString(blame));
                        _readStatus = ReadStatus.Reconfigure;
                        muxer.ReconfigureIfNeeded(blame, true, "broadcast");
                    }

                    // invoke the handlers
                    RedisChannel channel;
                    if (items[0].IsEqual(PhysicalConnectionHelpers.message))
                    {
                        channel = items[1].AsRedisChannel(_ChannelPrefix, RedisChannel.RedisChannelOptions.None);
                        Trace("MESSAGE: " + channel);
                    }
                    else // see check on outer-if that restricts to message / smessage
                    {
                        channel = items[1].AsRedisChannel(_ChannelPrefix, RedisChannel.RedisChannelOptions.Sharded);
                        Trace("SMESSAGE: " + channel);
                    }
                    if (!channel.IsNull)
                    {
                        if (TryGetPubSubPayload(items[2], out var payload))
                        {
                            _readStatus = ReadStatus.InvokePubSub;
                            muxer.OnMessage(channel, channel, payload);
                        }
                        // could be multi-message: https://github.com/StackExchange/StackExchange.Redis/issues/2507
                        else if (TryGetMultiPubSubPayload(items[2], out var payloads))
                        {
                            _readStatus = ReadStatus.InvokePubSub;
                            muxer.OnMessage(channel, channel, payloads);
                        }
                    }
                    return; // AND STOP PROCESSING!
                }
                else if (items.Length >= 4 && items[0].IsEqual(PhysicalConnectionHelpers.pmessage))
                {
                    _readStatus = ReadStatus.PubSubPMessage;

                    var channel = items[2].AsRedisChannel(_ChannelPrefix, RedisChannel.RedisChannelOptions.Pattern);

                    Trace("PMESSAGE: " + channel);
                    if (!channel.IsNull)
                    {
                        if (TryGetPubSubPayload(items[3], out var payload))
                        {
                            var sub = items[1].AsRedisChannel(_ChannelPrefix, RedisChannel.RedisChannelOptions.Pattern);

                            _readStatus = ReadStatus.InvokePubSub;
                            muxer.OnMessage(sub, channel, payload);
                        }
                        else if (TryGetMultiPubSubPayload(items[3], out var payloads))
                        {
                            var sub = items[1].AsRedisChannel(_ChannelPrefix, RedisChannel.RedisChannelOptions.Pattern);

                            _readStatus = ReadStatus.InvokePubSub;
                            muxer.OnMessage(sub, channel, payloads);
                        }
                    }
                    return; // AND STOP PROCESSING!
                }

                // if it didn't look like "[p|s]message", then we still need to process the pending queue
            }
            Trace("Matching result...");

            Message? msg = null;
            // check whether we're waiting for a high-integrity mode post-response checksum (using cheap null-check first)
            if (_awaitingToken is not null && (msg = Interlocked.Exchange(ref _awaitingToken, null)) is not null)
            {
                _readStatus = ReadStatus.ResponseSequenceCheck;
                if (!ProcessHighIntegrityResponseToken(msg, in result, BridgeCouldBeNull))
                {
                    RecordConnectionFailed(ConnectionFailureType.ResponseIntegrityFailure, origin: nameof(ReadStatus.ResponseSequenceCheck));
                }
                return;
            }

            _readStatus = ReadStatus.DequeueResult;
            lock (_writtenAwaitingResponse)
            {
                if (msg is not null)
                {
                    _awaitingToken = null;
                }

                if (!_writtenAwaitingResponse.TryDequeue(out msg))
                {
                    throw new InvalidOperationException("Received response with no message waiting: " + result.ToString());
                }
            }
            _activeMessage = msg;

            Trace("Response to: " + msg);
            _readStatus = ReadStatus.ComputeResult;
            if (msg.ComputeResult(this, result))
            {
                _readStatus = msg.ResultBoxIsAsync ? ReadStatus.CompletePendingMessageAsync : ReadStatus.CompletePendingMessageSync;
                if (!msg.IsHighIntegrity)
                {
                    // can't complete yet if needs checksum
                    msg.Complete();
                }
            }
            if (msg.IsHighIntegrity)
            {
                // stash this for the next non-OOB response
                Volatile.Write(ref _awaitingToken, msg);
            }

            _readStatus = ReadStatus.MatchResultComplete;
            _activeMessage = null;

            static bool ProcessHighIntegrityResponseToken(Message message, in RawResult result, PhysicalBridge? bridge)
            {
                bool isValid = false;
                if (result.Resp2TypeBulkString == ResultType.BulkString)
                {
                    var payload = result.Payload;
                    if (payload.Length == 4)
                    {
                        uint interpreted;
                        if (payload.IsSingleSegment)
                        {
                            interpreted = BinaryPrimitives.ReadUInt32LittleEndian(payload.First.Span);
                        }
                        else
                        {
                            Span<byte> span = stackalloc byte[4];
                            payload.CopyTo(span);
                            interpreted = BinaryPrimitives.ReadUInt32LittleEndian(span);
                        }
                        isValid = interpreted == message.HighIntegrityToken;
                    }
                }
                if (isValid)
                {
                    message.Complete();
                    return true;
                }
                else
                {
                    message.SetExceptionAndComplete(new InvalidOperationException("High-integrity mode detected possible protocol de-sync"), bridge);
                    return false;
                }
            }

            static bool TryGetPubSubPayload(in RawResult value, out RedisValue parsed, bool allowArraySingleton = true)
            {
                if (value.IsNull)
                {
                    parsed = RedisValue.Null;
                    return true;
                }
                switch (value.Resp2TypeBulkString)
                {
                    case ResultType.Integer:
                    case ResultType.SimpleString:
                    case ResultType.BulkString:
                        parsed = value.AsRedisValue();
                        return true;
                    case ResultType.Array when allowArraySingleton && value.ItemsCount == 1:
                        return TryGetPubSubPayload(in value[0], out parsed, allowArraySingleton: false);
                }
                parsed = default;
                return false;
            }

            static bool TryGetMultiPubSubPayload(in RawResult value, out Sequence<RawResult> parsed)
            {
                if (value.Resp2TypeArray == ResultType.Array && value.ItemsCount != 0)
                {
                    parsed = value.GetItems();
                    return true;
                }
                parsed = default;
                return false;
            }
        }

        private volatile Message? _activeMessage;

        public void GetHeadMessages(out Message? now, out Message? next)
        {
            now = _activeMessage;
            bool haveLock = false;
            try
            {
                // careful locking here; a: don't try too hard (this is error info only), b: avoid deadlock (see #2376)
                Monitor.TryEnter(_writtenAwaitingResponse, 10, ref haveLock);
                if (haveLock)
                {
                    _writtenAwaitingResponse.TryPeek(out next);
                }
                else
                {
                    next = UnknownMessage.Instance;
                }
            }
            finally
            {
                if (haveLock) Monitor.Exit(_writtenAwaitingResponse);
            }
        }

        partial void OnCloseEcho();

        partial void OnCreateEcho();

        private void OnDebugAbort()
        {
            var bridge = BridgeCouldBeNull;
            if (bridge == null || !bridge.Multiplexer.AllowConnect)
            {
                throw new RedisConnectionException(ConnectionFailureType.InternalFailure, "Aborting (AllowConnect: False)");
            }
        }

        partial void OnWrapForLogging(ref IDuplexPipe pipe, string name, SocketManager mgr);

        internal void UpdateLastReadTime() => Interlocked.Exchange(ref lastReadTickCount, Environment.TickCount);
        private async Task ReadFromPipe()
        {
            bool allowSyncRead = true, isReading = false;
            try
            {
                _readStatus = ReadStatus.Init;
                while (true)
                {
                    var input = _ioPipe?.Input;
                    if (input == null) break;

                    // note: TryRead will give us back the same buffer in a tight loop
                    // - so: only use that if we're making progress
                    isReading = true;
                    _readStatus = ReadStatus.ReadSync;
                    if (!(allowSyncRead && input.TryRead(out var readResult)))
                    {
                        _readStatus = ReadStatus.ReadAsync;
                        readResult = await input.ReadAsync().ForAwait();
                    }
                    isReading = false;
                    _readStatus = ReadStatus.UpdateWriteTime;
                    UpdateLastReadTime();

                    _readStatus = ReadStatus.ProcessBuffer;
                    var buffer = readResult.Buffer;
                    int handled = 0;
                    if (!buffer.IsEmpty)
                    {
                        handled = ProcessBuffer(ref buffer); // updates buffer.Start
                    }

                    allowSyncRead = handled != 0;

                    _readStatus = ReadStatus.MarkProcessed;
                    Trace($"Processed {handled} messages");
                    input.AdvanceTo(buffer.Start, buffer.End);

                    if (handled == 0 && readResult.IsCompleted)
                    {
                        break; // no more data, or trailing incomplete messages
                    }
                }
                Trace("EOF");
                RecordConnectionFailed(ConnectionFailureType.SocketClosed);
                _readStatus = ReadStatus.RanToCompletion;
            }
            catch (Exception ex)
            {
                _readStatus = ReadStatus.Faulted;
                // this CEX is just a hardcore "seriously, read the actual value" - there's no
                // convenient "Thread.VolatileRead<T>(ref T field) where T : class", and I don't
                // want to make the field volatile just for this one place that needs it
                if (isReading)
                {
                    var pipe = Volatile.Read(ref _ioPipe);
                    if (pipe == null)
                    {
                        return;
                        // yeah, that's fine... don't worry about it; we nuked it
                    }

                    // check for confusing read errors - no need to present "Reading is not allowed after reader was completed."
                    if (pipe is SocketConnection sc && sc.ShutdownKind == PipeShutdownKind.ReadEndOfStream)
                    {
                        RecordConnectionFailed(ConnectionFailureType.SocketClosed, new EndOfStreamException());
                        return;
                    }
                }
                Trace("Faulted");
                RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
            }
        }

        private static readonly ArenaOptions s_arenaOptions = new ArenaOptions();
        private readonly Arena<RawResult> _arena = new Arena<RawResult>(s_arenaOptions);

        private int ProcessBuffer(ref ReadOnlySequence<byte> buffer)
        {
            int messageCount = 0;
            bytesInBuffer = buffer.Length;

            while (!buffer.IsEmpty)
            {
                _readStatus = ReadStatus.TryParseResult;
                var reader = new BufferReader(buffer);
                var result = PhysicalConnectionHelpers.TryParseResult(_protocol >= RedisProtocol.Resp3, _arena, in buffer, ref reader, IncludeDetailInExceptions, this);
                try
                {
                    if (result.HasValue)
                    {
                        buffer = reader.SliceFromCurrent();

                        messageCount++;
                        Trace(result.ToString());
                        _readStatus = ReadStatus.MatchResult;
                        MatchResult(result);

                        // Track the last result size *after* processing for the *next* error message
                        bytesInBuffer = buffer.Length;
                        bytesLastResult = result.Payload.Length;
                    }
                    else
                    {
                        break; // remaining buffer isn't enough; give up
                    }
                }
                finally
                {
                    _readStatus = ReadStatus.ResetArena;
                    _arena.Reset();
                }
            }
            _readStatus = ReadStatus.ProcessBufferComplete;
            return messageCount;
        }

        private volatile ReadStatus _readStatus;
        public ReadStatus GetReadStatus() => _readStatus;

        internal void StartReading() => ReadFromPipe().RedisFireAndForget();

        public bool HasPendingCallerFacingItems()
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(_writtenAwaitingResponse, 0, ref lockTaken);
                if (lockTaken)
                {
                    if (_writtenAwaitingResponse.Count != 0)
                    {
                        foreach (var item in _writtenAwaitingResponse)
                        {
                            if (!item.IsInternalCall) return true;
                        }
                    }
                    return false;
                }
                else
                {
                    // don't contend the lock; *presume* that something
                    // qualifies; we can check again next heartbeat
                    return true;
                }
            }
            finally
            {
                if (lockTaken) Monitor.Exit(_writtenAwaitingResponse);
            }
        }
    }
}

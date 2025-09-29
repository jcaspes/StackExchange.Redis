using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Pipelines.Sockets.Unofficial.Arenas;

namespace StackExchange.Redis
{
    internal static class PhysicalConnectionHelpers
    {
        internal const int REDIS_MAX_ARGS = 1024 * 1024; // there is a <= 1024*1024 max constraint inside redis itself: https://github.com/antirez/redis/blob/6c60526db91e23fb2d666fc52facc9a11780a2a3/src/networking.c#L1024
        internal const int DefaultRedisDatabaseCount = 16;

        [ThreadStatic]
        private static Encoder? s_PerThreadEncoder;
        internal static Encoder GetPerThreadEncoder()
        {
            var encoder = s_PerThreadEncoder;
            if (encoder == null)
            {
                s_PerThreadEncoder = encoder = Encoding.UTF8.GetEncoder();
            }
            else
            {
                encoder.Reset();
            }
            return encoder;
        }

        internal static RawResult TryParseResult(
            bool isResp3,
            Arena<RawResult> arena,
            in ReadOnlySequence<byte> buffer,
            ref BufferReader reader,
            bool includeDetilInExceptions,
            IPhysicalConnection? connection,
            bool allowInlineProtocol = false)
        {
            return TryParseResult(
                isResp3 ? (RawResult.ResultFlags.Resp3 | RawResult.ResultFlags.NonNull) : RawResult.ResultFlags.NonNull,
                arena,
                buffer,
                ref reader,
                includeDetilInExceptions,
                connection?.BridgeCouldBeNull?.ServerEndPoint,
                allowInlineProtocol);
        }

        private static RawResult TryParseResult(
            RawResult.ResultFlags flags,
            Arena<RawResult> arena,
            in ReadOnlySequence<byte> buffer,
            ref BufferReader reader,
            bool includeDetilInExceptions,
            ServerEndPoint? server,
            bool allowInlineProtocol = false)
        {
            int prefix;
            do // this loop is just to allow us to parse (skip) attributes without doing a stack-dive
            {
                prefix = reader.PeekByte();
                if (prefix < 0) return RawResult.Nil; // EOF
                switch (prefix)
                {
                    // RESP2
                    case '+': // simple string
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.SimpleString, flags, ref reader);
                    case '-': // error
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.Error, flags, ref reader);
                    case ':': // integer
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.Integer, flags, ref reader);
                    case '$': // bulk string
                        reader.Consume(1);
                        return ReadBulkString(ResultType.BulkString, flags, ref reader, includeDetilInExceptions, server);
                    case '*': // array
                        reader.Consume(1);
                        return ReadArray(ResultType.Array, flags, arena, in buffer, ref reader, includeDetilInExceptions, server);
                    // RESP3
                    case '_': // null
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.Null, flags, ref reader);
                    case ',': // double
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.Double, flags, ref reader);
                    case '#': // boolean
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.Boolean, flags, ref reader);
                    case '!': // blob error
                        reader.Consume(1);
                        return ReadBulkString(ResultType.BlobError, flags, ref reader, includeDetilInExceptions, server);
                    case '=': // verbatim string
                        reader.Consume(1);
                        return ReadBulkString(ResultType.VerbatimString, flags, ref reader, includeDetilInExceptions, server);
                    case '(': // big number
                        reader.Consume(1);
                        return ReadLineTerminatedString(ResultType.BigInteger, flags, ref reader);
                    case '%': // map
                        reader.Consume(1);
                        return ReadArray(ResultType.Map, flags, arena, in buffer, ref reader, includeDetilInExceptions, server);
                    case '~': // set
                        reader.Consume(1);
                        return ReadArray(ResultType.Set, flags, arena, in buffer, ref reader, includeDetilInExceptions, server);
                    case '|': // attribute
                        reader.Consume(1);
                        var arr = ReadArray(ResultType.Attribute, flags, arena, in buffer, ref reader, includeDetilInExceptions, server);
                        if (!arr.HasValue) return RawResult.Nil; // failed to parse attribute data

                        // for now, we want to just skip attribute data; so
                        // drop whatever we parsed on the floor and keep looking
                        break; // exits the SWITCH, not the DO/WHILE
                    case '>': // push
                        reader.Consume(1);
                        return ReadArray(ResultType.Push, flags, arena, in buffer, ref reader, includeDetilInExceptions, server);
                }
            }
            while (prefix == '|');

            if (allowInlineProtocol) return ParseInlineProtocol(flags, arena, ReadLineTerminatedString(ResultType.SimpleString, flags, ref reader));
            throw new InvalidOperationException("Unexpected response prefix: " + (char)prefix);
        }

        private static RawResult ReadLineTerminatedString(ResultType type, RawResult.ResultFlags flags, ref BufferReader reader)
        {
            int crlfOffsetFromCurrent = BufferReader.FindNextCrLf(reader);
            if (crlfOffsetFromCurrent < 0) return RawResult.Nil;

            var payload = reader.ConsumeAsBuffer(crlfOffsetFromCurrent);
            reader.Consume(2);

            return new RawResult(type, payload, flags);
        }
        private static RawResult ReadBulkString(ResultType type, RawResult.ResultFlags flags, ref BufferReader reader, bool includeDetailInExceptions, ServerEndPoint? server)
        {
            var prefix = ReadLineTerminatedString(ResultType.Integer, flags, ref reader);
            if (prefix.HasValue)
            {
                if (!prefix.TryGetInt64(out long i64))
                {
                    throw ExceptionFactory.ConnectionFailure(
                        includeDetailInExceptions,
                        ConnectionFailureType.ProtocolFailure,
                        prefix.Is('?') ? "Streamed strings not yet implemented" : "Invalid bulk string length",
                        server);
                }
                int bodySize = checked((int)i64);
                if (bodySize < 0)
                {
                    return new RawResult(type, ReadOnlySequence<byte>.Empty, AsNull(flags));
                }

                if (reader.TryConsumeAsBuffer(bodySize, out var payload))
                {
                    switch (reader.TryConsumeCRLF())
                    {
                        case ConsumeResult.NeedMoreData:
                            break; // see NilResult below
                        case ConsumeResult.Success:
                            return new RawResult(type, payload, flags);
                        default:
                            throw ExceptionFactory.ConnectionFailure(includeDetailInExceptions, ConnectionFailureType.ProtocolFailure, "Invalid bulk string terminator", server);
                    }
                }
            }
            return RawResult.Nil;
        }
        private static RawResult.ResultFlags AsNull(RawResult.ResultFlags flags) => flags & ~RawResult.ResultFlags.NonNull;

        private static RawResult ReadArray(ResultType resultType, RawResult.ResultFlags flags, Arena<RawResult> arena, in ReadOnlySequence<byte> buffer, ref BufferReader reader, bool includeDetailInExceptions, ServerEndPoint? server)
        {
            var itemCount = ReadLineTerminatedString(ResultType.Integer, flags, ref reader);
            if (itemCount.HasValue)
            {
                if (!itemCount.TryGetInt64(out long i64))
                {
                    throw ExceptionFactory.ConnectionFailure(
                        includeDetailInExceptions,
                        ConnectionFailureType.ProtocolFailure,
                        itemCount.Is('?') ? "Streamed aggregate types not yet implemented" : "Invalid array length",
                        server);
                }

                int itemCountActual = checked((int)i64);

                if (itemCountActual < 0)
                {
                    // for null response by command like EXEC, RESP array: *-1\r\n
                    return new RawResult(resultType, items: default, AsNull(flags));
                }
                else if (itemCountActual == 0)
                {
                    // for zero array response by command like SCAN, Resp array: *0\r\n
                    return new RawResult(resultType, items: default, flags);
                }

                if (resultType == ResultType.Map) itemCountActual <<= 1; // if it says "3", it means 3 pairs, i.e. 6 values

                var oversized = arena.Allocate(itemCountActual);
                var result = new RawResult(resultType, oversized, flags);

                if (oversized.IsSingleSegment)
                {
                    var span = oversized.FirstSpan;
                    for (int i = 0; i < span.Length; i++)
                    {
                        if (!(span[i] = TryParseResult(flags, arena, in buffer, ref reader, includeDetailInExceptions, server)).HasValue)
                        {
                            return RawResult.Nil;
                        }
                    }
                }
                else
                {
                    foreach (var span in oversized.Spans)
                    {
                        for (int i = 0; i < span.Length; i++)
                        {
                            if (!(span[i] = TryParseResult(flags, arena, in buffer, ref reader, includeDetailInExceptions, server)).HasValue)
                            {
                                return RawResult.Nil;
                            }
                        }
                    }
                }
                return result;
            }
            return RawResult.Nil;
        }

        private static RawResult ParseInlineProtocol(RawResult.ResultFlags flags, Arena<RawResult> arena, in RawResult line)
        {
            if (!line.HasValue) return RawResult.Nil; // incomplete line

            int count = 0;
            foreach (var _ in line.GetInlineTokenizer()) count++;
            var block = arena.Allocate(count);

            var iter = block.GetEnumerator();
            foreach (var token in line.GetInlineTokenizer())
            {
                // this assigns *via a reference*, returned via the iterator; just... sweet
                iter.GetNext() = new RawResult(line.Resp3Type, token, flags); // spoof RESP2 from RESP1
            }
            return new RawResult(ResultType.Array, block, flags); // spoof RESP2 from RESP1
        }
        internal static RemoteCertificateValidationCallback? GetAmbientIssuerCertificateCallback()
        {
            try
            {
                var issuerPath = Environment.GetEnvironmentVariable("SERedis_IssuerCertPath");
                if (!string.IsNullOrEmpty(issuerPath)) return ConfigurationOptions.TrustIssuerCallback(issuerPath);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
            return null;
        }
        internal static LocalCertificateSelectionCallback? GetAmbientClientCertificateCallback()
        {
            try
            {
                var certificatePath = Environment.GetEnvironmentVariable("SERedis_ClientCertPfxPath");
                if (!string.IsNullOrEmpty(certificatePath) && File.Exists(certificatePath))
                {
                    var password = Environment.GetEnvironmentVariable("SERedis_ClientCertPassword");
                    var pfxStorageFlags = Environment.GetEnvironmentVariable("SERedis_ClientCertStorageFlags");
                    X509KeyStorageFlags storageFlags = X509KeyStorageFlags.DefaultKeySet;
                    if (!string.IsNullOrEmpty(pfxStorageFlags) && Enum.TryParse<X509KeyStorageFlags>(pfxStorageFlags, true, out var typedFlags))
                    {
                        storageFlags = typedFlags;
                    }

                    return ConfigurationOptions.CreatePfxUserCertificateCallback(certificatePath, password, storageFlags);
                }

#if NET5_0_OR_GREATER
                certificatePath = Environment.GetEnvironmentVariable("SERedis_ClientCertPemPath");
                if (!string.IsNullOrEmpty(certificatePath) && File.Exists(certificatePath))
                {
                    var passwordPath = Environment.GetEnvironmentVariable("SERedis_ClientCertPasswordPath");
                    return ConfigurationOptions.CreatePemUserCertificateCallback(certificatePath, passwordPath);
                }
#endif
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
            return null;
        }
        public static Message GetSelectDatabaseCommand(int targetDatabase)
        {
            return targetDatabase < DefaultRedisDatabaseCount
                   ? ReusableChangeDatabaseCommands[targetDatabase] // 0-15 by default
                   : Message.Create(targetDatabase, CommandFlags.FireAndForget, RedisCommand.SELECT);
        }

        internal static readonly CommandBytes message = "message", pmessage = "pmessage", smessage = "smessage";

        internal static readonly Message[] ReusableChangeDatabaseCommands = Enumerable.Range(0, DefaultRedisDatabaseCount).Select(
            i => Message.Create(i, CommandFlags.FireAndForget, RedisCommand.SELECT)).ToArray();

        internal static readonly Message
            ReusableReadOnlyCommand = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.READONLY),
            ReusableReadWriteCommand = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.READWRITE);

        public static int WriteRaw(Span<byte> span, long value, bool withLengthPrefix = false, int offset = 0)
        {
            if (value >= 0 && value <= 9)
            {
                if (withLengthPrefix)
                {
                    span[offset++] = (byte)'1';
                    offset = WriteCrlf(span, offset);
                }
                span[offset++] = (byte)((int)'0' + (int)value);
            }
            else if (value >= 10 && value < 100)
            {
                if (withLengthPrefix)
                {
                    span[offset++] = (byte)'2';
                    offset = WriteCrlf(span, offset);
                }
                span[offset++] = (byte)((int)'0' + ((int)value / 10));
                span[offset++] = (byte)((int)'0' + ((int)value % 10));
            }
            else if (value >= 100 && value < 1000)
            {
                int v = (int)value;
                int units = v % 10;
                v /= 10;
                int tens = v % 10, hundreds = v / 10;
                if (withLengthPrefix)
                {
                    span[offset++] = (byte)'3';
                    offset = WriteCrlf(span, offset);
                }
                span[offset++] = (byte)((int)'0' + hundreds);
                span[offset++] = (byte)((int)'0' + tens);
                span[offset++] = (byte)((int)'0' + units);
            }
            else if (value < 0 && value >= -9)
            {
                if (withLengthPrefix)
                {
                    span[offset++] = (byte)'2';
                    offset = WriteCrlf(span, offset);
                }
                span[offset++] = (byte)'-';
                span[offset++] = (byte)((int)'0' - (int)value);
            }
            else if (value <= -10 && value > -100)
            {
                if (withLengthPrefix)
                {
                    span[offset++] = (byte)'3';
                    offset = WriteCrlf(span, offset);
                }
                value = -value;
                span[offset++] = (byte)'-';
                span[offset++] = (byte)((int)'0' + ((int)value / 10));
                span[offset++] = (byte)((int)'0' + ((int)value % 10));
            }
            else
            {
                // we're going to write it, but *to the wrong place*
                var availableChunk = span.Slice(offset);
                var formattedLength = Format.FormatInt64(value, availableChunk);
                if (withLengthPrefix)
                {
                    // now we know how large the prefix is: write the prefix, then write the value
                    var prefixLength = Format.FormatInt32(formattedLength, availableChunk);
                    offset += prefixLength;
                    offset = WriteCrlf(span, offset);

                    availableChunk = span.Slice(offset);
                    var finalLength = Format.FormatInt64(value, availableChunk);
                    offset += finalLength;
                    Debug.Assert(finalLength == formattedLength);
                }
                else
                {
                    offset += formattedLength;
                }
            }

            return WriteCrlf(span, offset);
        }

        internal static void WriteMultiBulkHeader(PipeWriter output, long count)
        {
            // *{count}\r\n         = 3 + MaxInt32TextLen
            var span = output.GetSpan(3 + Format.MaxInt32TextLen);
            span[0] = (byte)'*';
            int offset = WriteRaw(span, count, offset: 1);
            output.Advance(offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int WriteCrlf(Span<byte> span, int offset)
        {
            span[offset++] = (byte)'\r';
            span[offset++] = (byte)'\n';
            return offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteCrlf(PipeWriter writer)
        {
            var span = writer.GetSpan(2);
            span[0] = (byte)'\r';
            span[1] = (byte)'\n';
            writer.Advance(2);
        }

        internal static unsafe void WriteRaw(PipeWriter writer, string value, int expectedLength)
        {
            const int MaxQuickEncodeSize = 512;

            fixed (char* cPtr = value)
            {
                int totalBytes;
                if (expectedLength <= MaxQuickEncodeSize)
                {
                    // encode directly in one hit
                    var span = writer.GetSpan(expectedLength);
                    fixed (byte* bPtr = span)
                    {
                        totalBytes = Encoding.UTF8.GetBytes(cPtr, value.Length, bPtr, expectedLength);
                    }
                    writer.Advance(expectedLength);
                }
                else
                {
                    // use an encoder in a loop
                    var encoder = GetPerThreadEncoder();
                    int charsRemaining = value.Length, charOffset = 0;
                    totalBytes = 0;

                    bool final = false;
                    while (true)
                    {
                        var span = writer.GetSpan(5); // get *some* memory - at least enough for 1 character (but hopefully lots more)

                        int charsUsed, bytesUsed;
                        bool completed;
                        fixed (byte* bPtr = span)
                        {
                            encoder.Convert(cPtr + charOffset, charsRemaining, bPtr, span.Length, final, out charsUsed, out bytesUsed, out completed);
                        }
                        writer.Advance(bytesUsed);
                        totalBytes += bytesUsed;
                        charOffset += charsUsed;
                        charsRemaining -= charsUsed;

                        if (charsRemaining <= 0)
                        {
                            if (charsRemaining < 0) throw new InvalidOperationException("String encode went negative");
                            if (completed) break; // fine
                            if (final) throw new InvalidOperationException("String encode failed to complete");
                            final = true; // flush the encoder to one more span, then exit
                        }
                    }
                }
                if (totalBytes != expectedLength) throw new InvalidOperationException("String encode length check failure");
            }
        }
        internal static int AppendToSpanCommand(Span<byte> span, in CommandBytes value, int offset = 0)
        {
            span[offset++] = (byte)'$';
            int len = value.Length;
            offset = WriteRaw(span, len, offset: offset);
            value.CopyTo(span.Slice(offset, len));
            offset += value.Length;
            return WriteCrlf(span, offset);
        }

        private static int AppendToSpan(Span<byte> span, ReadOnlySpan<byte> value, int offset = 0)
        {
            offset = WriteRaw(span, value.Length, offset: offset);
            value.CopyTo(span.Slice(offset, value.Length));
            offset += value.Length;
            return WriteCrlf(span, offset);
        }

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

        internal static readonly ReadOnlyMemory<byte> NullBulkString = Encoding.ASCII.GetBytes("$-1\r\n"), EmptyBulkString = Encoding.ASCII.GetBytes("$0\r\n\r\n");

        internal static byte ToHexNibble(int value)
        {
            return value < 10 ? (byte)('0' + value) : (byte)('a' - 10 + value);
        }

        internal static void WriteUnifiedPrefixedString(PipeWriter? maybeNullWriter, byte[]? prefix, string? value)
        {
            if (maybeNullWriter is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            if (value == null)
            {
                // special case
                writer.Write(NullBulkString.Span);
            }
            else
            {
                // ${total-len}\r\n         3 + MaxInt32TextLen
                // {prefix}{value}\r\n
                int encodedLength = Encoding.UTF8.GetByteCount(value),
                    prefixLength = prefix?.Length ?? 0,
                    totalLength = prefixLength + encodedLength;

                if (totalLength == 0)
                {
                    // special-case
                    writer.Write(EmptyBulkString.Span);
                }
                else
                {
                    var span = writer.GetSpan(3 + Format.MaxInt32TextLen);
                    span[0] = (byte)'$';
                    int bytes = WriteRaw(span, totalLength, offset: 1);
                    writer.Advance(bytes);

                    if (prefixLength != 0) writer.Write(prefix);
                    if (encodedLength != 0) WriteRaw(writer, value, encodedLength);
                    WriteCrlf(writer);
                }
            }
        }

        internal static void WriteUnifiedPrefixedBlob(PipeWriter? maybeNullWriter, byte[]? prefix, byte[]? value)
        {
            if (maybeNullWriter is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            // ${total-len}\r\n
            // {prefix}{value}\r\n
            if (prefix == null || prefix.Length == 0 || value == null)
            {
                // if no prefix, just use the non-prefixed version;
                // even if prefixed, a null value writes as null, so can use the non-prefixed version
                WriteUnifiedBlob(writer, value);
            }
            else
            {
                var span = writer.GetSpan(3 + Format.MaxInt32TextLen); // note even with 2 max-len, we're still in same text range
                span[0] = (byte)'$';
                int bytes = WriteRaw(span, prefix.LongLength + value.LongLength, offset: 1);
                writer.Advance(bytes);

                writer.Write(prefix);
                writer.Write(value);

                span = writer.GetSpan(2);
                WriteCrlf(span, 0);
                writer.Advance(2);
            }
        }

        internal static void WriteUnifiedInt64(PipeWriter writer, long value)
        {
            // note from specification: A client sends to the Redis server a RESP Array consisting of just Bulk Strings.
            // (i.e. we can't just send ":123\r\n", we need to send "$3\r\n123\r\n"

            // ${asc-len}\r\n           = 3 + MaxInt32TextLen
            // {asc}\r\n                = MaxInt64TextLen + 2
            var span = writer.GetSpan(5 + Format.MaxInt32TextLen + Format.MaxInt64TextLen);

            span[0] = (byte)'$';
            var bytes = WriteRaw(span, value, withLengthPrefix: true, offset: 1);
            writer.Advance(bytes);
        }

        internal static void WriteUnifiedUInt64(PipeWriter writer, ulong value)
        {
            // note from specification: A client sends to the Redis server a RESP Array consisting of just Bulk Strings.
            // (i.e. we can't just send ":123\r\n", we need to send "$3\r\n123\r\n"

            // ${asc-len}\r\n           = 3 + MaxInt32TextLen
            // {asc}\r\n                = MaxInt64TextLen + 2
            var span = writer.GetSpan(5 + Format.MaxInt32TextLen + Format.MaxInt64TextLen);

            Span<byte> valueSpan = stackalloc byte[Format.MaxInt64TextLen];
            var len = Format.FormatUInt64(value, valueSpan);
            span[0] = (byte)'$';
            int offset = WriteRaw(span, len, withLengthPrefix: false, offset: 1);
            valueSpan.Slice(0, len).CopyTo(span.Slice(offset));
            offset += len;
            offset = WriteCrlf(span, offset);
            writer.Advance(offset);
        }
        internal static void WriteInteger(PipeWriter writer, long value)
        {
            // note: client should never write integer; only server does this
            // :{asc}\r\n                = MaxInt64TextLen + 3
            var span = writer.GetSpan(3 + Format.MaxInt64TextLen);

            span[0] = (byte)':';
            var bytes = WriteRaw(span, value, withLengthPrefix: false, offset: 1);
            writer.Advance(bytes);
        }
        internal static void WriteUnifiedBlob(PipeWriter writer, byte[]? value)
        {
            if (value == null)
            {
                // special case:
                writer.Write(NullBulkString.Span);
            }
            else
            {
                WriteUnifiedSpan(writer, new ReadOnlySpan<byte>(value));
            }
        }

        internal static void WriteUnifiedSpan(PipeWriter writer, ReadOnlySpan<byte> value)
        {
            // ${len}\r\n           = 3 + MaxInt32TextLen
            // {value}\r\n          = 2 + value.Length
            const int MaxQuickSpanSize = 512;
            if (value.Length == 0)
            {
                // special case:
                writer.Write(EmptyBulkString.Span);
            }
            else if (value.Length <= MaxQuickSpanSize)
            {
                var span = writer.GetSpan(5 + Format.MaxInt32TextLen + value.Length);
                span[0] = (byte)'$';
                int bytes = AppendToSpan(span, value, 1);
                writer.Advance(bytes);
            }
            else
            {
                // too big to guarantee can do in a single span
                var span = writer.GetSpan(3 + Format.MaxInt32TextLen);
                span[0] = (byte)'$';
                int bytes = WriteRaw(span, value.Length, offset: 1);
                writer.Advance(bytes);

                writer.Write(value);

                WriteCrlf(writer);
            }
        }

        internal static void WriteUnifiedDouble(PipeWriter writer, double value)
        {
#if NET8_0_OR_GREATER
            Span<byte> valueSpan = stackalloc byte[Format.MaxDoubleTextLen];
            var len = Format.FormatDouble(value, valueSpan);

            // ${asc-len}\r\n           = 4/5 (asc-len at most 2 digits)
            // {asc}\r\n                = {len} + 2
            var span = writer.GetSpan(7 + len);
            span[0] = (byte)'$';
            int offset = WriteRaw(span, len, withLengthPrefix: false, offset: 1);
            valueSpan.Slice(0, len).CopyTo(span.Slice(offset));
            offset += len;
            offset = WriteCrlf(span, offset);
            writer.Advance(offset);
#else
            // fallback: drop to string
            WriteUnifiedPrefixedString(writer, null, Format.ToString(value));
#endif
        }
        public static void WriteBulkString(in RedisValue value, PipeWriter? maybeNullWriter)
        {
            if (maybeNullWriter is not PipeWriter writer)
            {
                return; // Prevent null refs during disposal
            }

            switch (value.Type)
            {
                case RedisValue.StorageType.Null:
                    WriteUnifiedBlob(writer, (byte[]?)null);
                    break;
                case RedisValue.StorageType.Int64:
                    WriteUnifiedInt64(writer, value.OverlappedValueInt64);
                    break;
                case RedisValue.StorageType.UInt64:
                    WriteUnifiedUInt64(writer, value.OverlappedValueUInt64);
                    break;
                case RedisValue.StorageType.Double:
                    WriteUnifiedDouble(writer, value.OverlappedValueDouble);
                    break;
                case RedisValue.StorageType.String:
                    WriteUnifiedPrefixedString(writer, null, (string?)value);
                    break;
                case RedisValue.StorageType.Raw:
                    WriteUnifiedSpan(writer, ((ReadOnlyMemory<byte>)value).Span);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected {value.Type} value: '{value}'");
            }
        }
    }
}

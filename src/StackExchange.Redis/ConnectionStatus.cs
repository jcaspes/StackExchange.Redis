namespace StackExchange.Redis
{
    internal readonly struct ConnectionStatus
    {
        /// <summary>
        /// Number of messages sent outbound, but we don't yet have a response for.
        /// </summary>
        public int MessagesSentAwaitingResponse { get; init; }

        /// <summary>
        /// Bytes available on the socket, not yet read into the pipe.
        /// </summary>
        public long BytesAvailableOnSocket { get; init; }

        /// <summary>
        /// Bytes read from the socket, pending in the reader pipe.
        /// </summary>
        public long BytesInReadPipe { get; init; }

        /// <summary>
        /// Bytes in the writer pipe, waiting to be written to the socket.
        /// </summary>
        public long BytesInWritePipe { get; init; }

        /// <summary>
        /// Byte size of the last result we processed.
        /// </summary>
        public long BytesLastResult { get; init; }

        /// <summary>
        /// Byte size on the buffer that isn't processed yet.
        /// </summary>
        public long BytesInBuffer { get; init; }

        /// <summary>
        /// The inbound pipe reader status.
        /// </summary>
        public ReadStatus ReadStatus { get; init; }

        /// <summary>
        /// The outbound pipe writer status.
        /// </summary>
        public WriteStatus WriteStatus { get; init; }

        public override string ToString() =>
            $"SentAwaitingResponse: {MessagesSentAwaitingResponse}, AvailableOnSocket: {BytesAvailableOnSocket} byte(s), InReadPipe: {BytesInReadPipe} byte(s), InWritePipe: {BytesInWritePipe} byte(s), ReadStatus: {ReadStatus}, WriteStatus: {WriteStatus}";

        /// <summary>
        /// The default connection stats, notable *not* the same as <c>default</c> since initializers don't run.
        /// </summary>
        public static ConnectionStatus Default { get; } = new()
        {
            BytesAvailableOnSocket = -1,
            BytesInReadPipe = -1,
            BytesInWritePipe = -1,
            ReadStatus = ReadStatus.NA,
            WriteStatus = WriteStatus.NA,
        };

        /// <summary>
        /// The zeroed connection stats, which we want to display as zero for default exception cases.
        /// </summary>
        public static ConnectionStatus Zero { get; } = new()
        {
            BytesAvailableOnSocket = 0,
            BytesInReadPipe = 0,
            BytesInWritePipe = 0,
            ReadStatus = ReadStatus.NA,
            WriteStatus = WriteStatus.NA,
        };
    }
}

namespace StackExchange.Redis
{
    internal enum ReadStatus
    {
        NotStarted,
        Init,
        RanToCompletion,
        Faulted,
        ReadSync,
        ReadAsync,
        UpdateWriteTime,
        ProcessBuffer,
        MarkProcessed,
        TryParseResult,
        MatchResult,
        PubSubMessage,
        PubSubPMessage,
        PubSubSMessage,
        Reconfigure,
        InvokePubSub,
        ResponseSequenceCheck, // high-integrity mode only
        DequeueResult,
        ComputeResult,
        CompletePendingMessageSync,
        CompletePendingMessageAsync,
        MatchResultComplete,
        ResetArena,
        ProcessBufferComplete,
        NA = -1,
    }
}

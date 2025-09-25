namespace StackExchange.Redis
{
    internal enum WriteStatus
    {
        Initializing,
        Idle,
        Writing,
        Flushing,
        Flushed,

        NA = -1,
    }
}

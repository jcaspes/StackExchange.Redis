namespace StackExchange.Redis
{
    internal class Header
    {
        public Header()
        {
        }
        internal RedisCommand command;
        internal int arguments;
        internal CommandBytes commandBytes = default;
    }
}

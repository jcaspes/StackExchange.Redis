using System.Collections.Generic;

namespace StackExchange.Redis
{
    internal interface IMultiMessage
    {
        IEnumerable<Message> GetMessages(IPhysicalConnection connection);
    }
}

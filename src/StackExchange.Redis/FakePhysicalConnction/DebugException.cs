using System;

namespace StackExchange.Redis
{
    internal class DebugException : Exception
    {
        public DebugException(string identifier, int currentconnectionIndex) : base()
        {
            try
            {
                Console.WriteLine($"Trace(Th:{identifier}:Cx{currentconnectionIndex}) DebugException: {Environment.StackTrace.ToString()}");
            }
            catch (OutOfMemoryException)
            {
                // we don't want to perturbate test is an outofmemory exception occure here
            }
        }
        public DebugException(string identifier, int currentconnectionIndex, string message) : base()
        {
            try
            {
                Console.WriteLine($"Trace(Th:{identifier}:Cx{currentconnectionIndex}) DebugException '{message}': {Environment.StackTrace.ToString()}");
            }
            catch (OutOfMemoryException)
            {
                // we don't want to perturbate test is an outofmemory exception occure here
            }
        }
    }
}

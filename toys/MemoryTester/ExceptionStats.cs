using System;
using System.Collections.Generic;

namespace MemoryTester
{
    internal class ExceptionStats
    {
        private readonly Dictionary<string, int> _exceptionCounts = new Dictionary<string, int>();
        public int outOfMemoryExceptionCount = 0;

        internal void Add(Exception ex)
        {
            if (ex is OutOfMemoryException)
            {
                System.Threading.Interlocked.Increment(ref outOfMemoryExceptionCount);
            }
            string exceptionName = ex.GetType().Name;
            string stackHash = ex.StackTrace?.GetHashCode().ToString() ?? "NoStackTrace";
            string key = $"{exceptionName}_{stackHash}";
            if (_exceptionCounts.ContainsKey(key))
            {
                _exceptionCounts[key]++;
                Threads.TraceConsole($"Th:{System.Threading.Thread.CurrentThread.ManagedThreadId} Key:{key}", inConsole: false);
            }
            else
            {
                _exceptionCounts[key] = 1;
                Threads.TraceConsole($"Th:{System.Threading.Thread.CurrentThread.ManagedThreadId} Key:{key}", inConsole: false);
                Threads.TraceConsole(ex.StackTrace, inConsole: false);
            }
        }

        internal void TraceStats()
        {
            foreach (var kvp in _exceptionCounts)
            {
                string message = $"Exception: {kvp.Key}, Count: {kvp.Value}";
                Threads.TraceConsole(message);
            }
        }
    }
}

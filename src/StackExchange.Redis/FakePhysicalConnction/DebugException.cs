using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace StackExchange.Redis
{
    internal class DebugException : Exception
    {
        public DebugException() : base()
        {
            Console.WriteLine($"DebugException: {Environment.StackTrace.ToString()}");
        }
        public DebugException(string message) : base()
        {
            Console.WriteLine($"DebugException '{message}': {Environment.StackTrace.ToString()}");
        }
    }
}

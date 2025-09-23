using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace MemoryTester
{
    public sealed class Threads
    {
        public static int redisCallsCount;
        public int count = 1;
        public bool stopAllThreads = false;
        public delegate void Callback(int threadIndex);
        private static StreamWriter logger;
        public static bool traceInFile = false;

        public static void Init()
        {
            if (traceInFile)
            {
                string logPath = $".\\log.{System.DateTime.Now.Ticks}.txt";
                logger = new StreamWriter(logPath, append: false);
                string startMessage = $"Start logging at {System.DateTime.Now} in file: '{logPath}'";
                logger.WriteLine(startMessage);
                Console.WriteLine(startMessage);
                Console.WriteLine();
            }
        }

        public static void TraceConsole(string text, bool inConsole = true)
        {
            if (inConsole || traceInFile)
            {
                string log = $"{Thread.CurrentThread.ManagedThreadId.ToString()} : {text}";
                if (inConsole)
                {
                    Console.WriteLine(log);
                }
                if (traceInFile)
                {
                    logger.WriteLine(log);
                    logger.Flush();
                }
            }
        }

        private static readonly Dictionary<string, Thread> threads = new();

        private int threadId = 0;
        public void Loop(Callback callback)
        {
            while (!stopAllThreads)
            {
                Console.WriteLine($"===> Threads loop, count: {threads.Count}, redis calls: {Threads.redisCallsCount} <===");
                // Create the wanted threads count
                while (threads.Count < count)
                {
                    Thread thread = new Thread(ThreadCallBack(callback))
                    {
                        Name = threadId.ToString(),
                    };
                    threads.Add(thread.Name, thread);
                    thread.Start(threadId++);
                }
                // Check for dead threads and reloop
                List<string> deadThreads = new();
                foreach (var threadKVP in threads)
                {
                    if (!threadKVP.Value.IsAlive)
                    {
                        deadThreads.Add(threadKVP.Key);
                    }
                }

                foreach (var deadThread in deadThreads)
                {
                    threads.Remove(deadThread);
                }

                Thread.Sleep(1000);
            }
        }

        private static ParameterizedThreadStart ThreadCallBack(Callback callback) => (object obj) =>
        {
            try
            {
                callback((int)obj);
            }
            catch (Exception) // exception)
            {
                // TraceConsole($"Stop thread: {exception.GetType().FullName} occured.");
            }
        };
    }
}

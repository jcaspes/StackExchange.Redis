using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace MemoryTester
{
    public sealed class Threads
    {
        public int count = 1;
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

        public void Loop(Callback callback)
        {
            List<Thread> threads = new List<Thread>(count);

            for (int iThread = 0; iThread < count; iThread++)
            {
                Thread thread = new Thread(ThreadCallBack(callback));
                threads.Add(thread);
            }

            for (int iThread = 0; iThread < count; iThread++)
            {
                Thread thread = threads[iThread];
                thread.Start(iThread);
            }
            foreach (Thread thread in threads)
            {
                thread.Join();
            }
        }

        private static ParameterizedThreadStart ThreadCallBack(Callback callback) => (object obj) =>
        {
            try
            {
                int threadId = -1;
                if (obj != null)
                {
                    threadId = (int)obj;
                }
                callback(threadId);
            }
            catch (Exception)
            {
                // TraceConsole($"Stop thread: {exception.GetType().FullName} occured : {exception.Message}\r\n{exception.StackTrace}");
            }
        };
    }
}

using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using StackExchange.Redis;

namespace MemoryTester
{
    internal class Program
    {
        private static int corruptedDataCount;
        private static int corruptedDataSizeCount;
        private static int keyNotFoundCount;
        private static bool checkContent = false;
        private static double maxSize = 0x24000; // max size of string
        private static int threadCount = 200;
        private static bool highIntegrity = false;
        private static bool cleanPreviousRun = true;
        private static bool generateOOM = true;
        private static TimeSpan testDuration = TimeSpan.FromHours(24);

        private static void Main(string[] args)
        {
            if (args.Length > 0)
            {
                foreach (string arg in args)
                {
                    if (arg.Equals("checkContent", StringComparison.OrdinalIgnoreCase))
                        checkContent = true;
                    if (arg.Equals("log", StringComparison.OrdinalIgnoreCase))
                        Threads.traceInFile = true;
                    if (arg.StartsWith("maxsize", StringComparison.OrdinalIgnoreCase))
                    {
                        string[] argSplit = arg.Split('=');
                        maxSize = double.Parse(argSplit[1]);
                    }
                    if (arg.Equals("highIntegrity", StringComparison.OrdinalIgnoreCase))
                        highIntegrity = true;
                    if (arg.Equals("noClean", StringComparison.OrdinalIgnoreCase))
                        cleanPreviousRun = false;
                    if (arg.StartsWith("threads", StringComparison.OrdinalIgnoreCase))
                    {
                        string[] argSplit = arg.Split('=');
                        threadCount = int.Parse(argSplit[1]);
                    }
                    if (arg.Equals("noOOM", StringComparison.OrdinalIgnoreCase))
                        generateOOM = false;

                    if (arg.StartsWith("duration", StringComparison.OrdinalIgnoreCase))
                    {
                        string[] argSplit = arg.Split('=');
                        testDuration = TimeSpan.FromSeconds(int.Parse(argSplit[1]));
                    }
                }
            }

            System.Threading.Thread.CurrentThread.Name = "Main";

            Threads.Init();

            Threads.TraceConsole("-----------------------------------------------");
            Threads.TraceConsole($"Starting the tests (press any key to stop)");
            Threads.TraceConsole($"\tCheckContent    : {checkContent}");
            Threads.TraceConsole($"\tLog             : {Threads.traceInFile}");
            Threads.TraceConsole($"\tMaxsize         : {maxSize}");
            Threads.TraceConsole($"\tHighIntegrity   : {highIntegrity}");
            Threads.TraceConsole($"\tCleanPreviousRun: {cleanPreviousRun}");
            Threads.TraceConsole($"\tThreads         : {threadCount}");
            Threads.TraceConsole($"\tOOM             : {generateOOM}");
            Threads.TraceConsole($"\tduration        : {testDuration.TotalSeconds}");
            Threads.TraceConsole("-----------------------------------------------");

            ConfigurationOptions configuration = new ConfigurationOptions()
            {
                EndPoints = { System.Configuration.ConfigurationManager.AppSettings["redisHost"] },
                Password = System.Configuration.ConfigurationManager.AppSettings["redisPassword"],
                DefaultDatabase = 1,
                AllowAdmin = true,
                AbortOnConnectFail = true,
                SyncTimeout = 5000,
                HighIntegrity = highIntegrity,
                HeartbeatInterval = TimeSpan.FromHours(24), // so no heartbeat
                CommandMap = CommandMap.Create(new Dictionary<string, string> { { "SUBSCRIBE", string.Empty } }),
            };

            ExceptionStats exceptionStats = new ExceptionStats();

            ConnectionMultiplexer connection = ConnectionMultiplexer.Connect(configuration);

            if (!connection.IsConnected)
            {
                Threads.TraceConsole("No connection to redis, exiting");
                return;
            }
            else
            {
                Threads.TraceConsole("Connected to redis");
            }

            Threads threads = new Threads
            {
                count = threadCount,
            };

            // Unique key base on process id so multiple instance of the tester can run on the same redis server
            string processId = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string BaseKeyName = $"MemoryTester:pid{processId}";

            IDatabase redisDbForClean = connection.GetDatabase();
            IEnumerable<RedisKey> resultKeysEnumerable;
            TimeSpan duration = redisDbForClean.Ping();
            Threads.TraceConsole($"Ping duration {duration.TotalMilliseconds} ms");

            if (cleanPreviousRun)
            {
                resultKeysEnumerable = connection.GetServers()[0].Keys((int)configuration.DefaultDatabase, pattern: $"MemoryTester:*");
                foreach (RedisKey key in resultKeysEnumerable)
                {
                    redisDbForClean.KeyDelete(key);
                }
            }

            Stopwatch stopwatch = Stopwatch.StartNew();

            threads.Loop((int threadIndex) =>
            {
                // A list to memoryse loop data and consume memory
                List<string> results = new List<string>();

                string bigString = "";
                string key = $"Th{System.Threading.Thread.CurrentThread.Name}hT";

                // construct a big random string for current thread
                // set this string in redis so when can test HGET on high memory pressure
                string valueInRedis = ">>>!" + StringsHelpers.RandomString((int)(new Random(threadIndex).NextDouble() * maxSize), threadIndex) + "!<<<";
                int expectedSize = valueInRedis.Length;
                string objectUid = $"{BaseKeyName}:{key}";

                HashEntry[] hashEntries =
                {
                    new HashEntry("Value", valueInRedis),
                    new HashEntry("thread", key),
                };

                IDatabase redis = connection.GetDatabase();
                redis.HashSet(objectUid, hashEntries);
                redis.KeyExpire(objectUid, TimeSpan.FromHours(24)); // to auto clean redis afdter testing

                // loop to fill memory with redis data and try to get OutOffMemory exceptions
                while (!threads.stopAllThreads)
                {
                    try
                    {
                        if (!threads.stopAllThreads)
                        {
                            if (Console.KeyAvailable || stopwatch.Elapsed > testDuration)
                            {
                                Console.WriteLine("Exiting the test loop.");
                                threads.stopAllThreads = true;
                            }
                        }

                        HashEntry[] hashEntriesFromRedis;
                        try
                        {
                            hashEntriesFromRedis = redis.HashGetAll(objectUid);
                        }
                        finally
                        {
                            System.Threading.Interlocked.Increment(ref Threads.redisCallsCount);
                        }

                        // All keys must be found, if not it is an error, we expect an exception
                        if (hashEntriesFromRedis == null || hashEntriesFromRedis.Length == 0)
                        {
                            System.Threading.Interlocked.Increment(ref keyNotFoundCount);
                            try
                            {
                                Threads.TraceConsole($"Key '{key}' not found");
                            }
                            catch { }
                            continue;
                        }

                        IDictionary<string, string> props = hashEntriesFromRedis.ToStringDictionary();
                        // Check if content is not corrupted by looking prefix and suffix
                        bool valueIsValid = props["Value"].EndsWith("!<<<") && props["Value"].StartsWith(">>>!");
                        // Check data integrity, first check if received data is from current thread by comparing key
                        if (key != props["thread"])
                        {
                            System.Threading.Interlocked.Increment(ref corruptedDataCount);
                            try
                            {
                                Threads.TraceConsole($"Error in thread field, expected:{key}, actual:{props["thread"]}, value is valid but from an other query: {valueIsValid}");
                            }
                            catch { }
                        }

                        // Check data integrity, first check if received data is from current thread by comparing key
                        if (checkContent && expectedSize != props["Value"].Length)
                        {
                            System.Threading.Interlocked.Increment(ref corruptedDataSizeCount);
                            try
                            {
                                Threads.TraceConsole($"Expected:{valueInRedis.Substring(0, 25)}...");
                                Threads.TraceConsole($"inRedis :{props["Value"].Substring(0, 25)}...");
                                Threads.TraceConsole($"Error in value field lenght in thread '{key}', expected:{expectedSize}, actual:{props["Value"].Length}, value is valid but from an other query: {valueIsValid}");
                            }
                            catch { }
                        }

                        if (generateOOM)
                        {
                            // generate a big string to consume memory and generate out of memory exceptions
                            // this string will be freed by GC so we can continue to run
                            bigString = string.Join("-", results);
                            // if we have no OutOfMemory exception, we can add the value to results
                            results.Add(props["Value"]);
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptionStats.Add(ex);
                    }
                }
            });

            stopwatch.Stop();

            // Wait for all threads to end to avoid conflict in Exceptions stats dictionnary
            threads.WaitAll();

            Threads.TraceConsole("");
            Threads.TraceConsole("-------------------------------");
            exceptionStats.TraceStats();
            Threads.TraceConsole($"Total duration:{stopwatch.Elapsed}");
            Threads.TraceConsole($"Total redis calls:{Threads.redisCallsCount}");
            Threads.TraceConsole($"Total redis calls without exception:{Threads.redisCallsCount - ExceptionStats.ExceptionCount}");
            Threads.TraceConsole($"Success redis calls (without exception and with correct data):{Threads.redisCallsCount - ExceptionStats.ExceptionCount - corruptedDataCount - corruptedDataSizeCount - keyNotFoundCount}");
            Threads.TraceConsole($"Corrupted redis commands key value: {corruptedDataCount}");
            if (checkContent)
            {
                Threads.TraceConsole($"Corrupted redis commands data size: {corruptedDataSizeCount}");
            }
            Threads.TraceConsole($"Key not found unexpected errors {keyNotFoundCount}");
            Threads.TraceConsole($"Time in wait: {ConnectionMultiplexer.elapsedMilliSecondsInWait} ms");
        }
    }
}

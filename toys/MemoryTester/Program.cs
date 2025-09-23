using System;
using System.Collections.Generic;
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
        private static bool highIntegrity = false;
        private static bool cleanPreviousRun = true;

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
                }
            }

            Threads.Init();

            Threads.TraceConsole($"Starting the tests (press any key to stop)");

            ConfigurationOptions configuration = new ConfigurationOptions()
            {
                EndPoints = { System.Configuration.ConfigurationManager.AppSettings["redisHost"] },
                Password = System.Configuration.ConfigurationManager.AppSettings["redisPassword"],
                DefaultDatabase = 1,
                AllowAdmin = true,
                AbortOnConnectFail = false,
                SyncTimeout = 5000,
                HighIntegrity = highIntegrity,
            };

            Console.WriteLine(configuration);

            ExceptionStats exceptionStats = new ExceptionStats();
            ConnectionMultiplexer connection = ConnectionMultiplexer.Connect(configuration);

            bool contentLogged = false;

            Threads threads = new Threads
            {
                count = 200,
            };

            // Unique key base on process id so multiple instance of the tester can run on the same redis server
            string processId = System.Diagnostics.Process.GetCurrentProcess().Id.ToString();
            string BaseKeyName = $"MemoryTester:pid{processId}";

            IDatabase redis = connection.GetDatabase();
            IEnumerable<RedisKey> resultKeysEnumerable;

            if (cleanPreviousRun)
            {
                resultKeysEnumerable = connection.GetServers()[0].Keys((int)configuration.DefaultDatabase, pattern: $"MemoryTester:*");
                foreach (RedisKey key in resultKeysEnumerable)
                {
                    redis.KeyDelete(key);
                }
            }

            threads.Loop((int threadIndex) =>
            {
                // A list to memoryse loop data and consume memory
                List<string> results = new List<string>();

                string bigString = "";
                string key = $"Th{System.Threading.Thread.CurrentThread.ManagedThreadId}hT";

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

                redis.HashSet(objectUid, hashEntries);
                redis.KeyExpire(objectUid, TimeSpan.FromHours(24)); // to auto clean redis afdter testing

                // loop to fill memory with redis data and try to get OutOffMemory exceptions
                while (!threads.stopAllThreads)
                {
                    try
                    {
                        if (!threads.stopAllThreads)
                        {
                            if (Console.KeyAvailable)
                            {
                                Console.WriteLine("Exiting the test loop.");
                                threads.stopAllThreads = true;
                            }
                        }

                        HashEntry[] hashEntriesFromRedis = redis.HashGetAll(objectUid);
                        System.Threading.Interlocked.Increment(ref Threads.redisCallsCount);
                        if (hashEntriesFromRedis == null || hashEntriesFromRedis.Length == 0)
                        {
                            Threads.TraceConsole($"Key '{key}' not found");
                            System.Threading.Interlocked.Increment(ref keyNotFoundCount);
                            continue;
                        }

                        IDictionary<string, string> props = hashEntriesFromRedis.ToStringDictionary();
                        bool valueIsValid = props["Value"].EndsWith("!<<<") && props["Value"].StartsWith(">>>!");
                        if (key != props["thread"])
                        {
                            System.Threading.Interlocked.Increment(ref corruptedDataCount);
                            Threads.TraceConsole($"Error in thread field, expected:{key}, actual:{props["thread"]}, value is valid but from an other query: {valueIsValid}");
                        }
                        if (checkContent && expectedSize != props["Value"].Length)
                        {
                            System.Threading.Interlocked.Increment(ref corruptedDataSizeCount);
                            if (!contentLogged)
                            {
                                contentLogged = true;
                                Threads.TraceConsole($"{valueInRedis.Substring(0, 25)}...");
                                Threads.TraceConsole($"{props["Value"].Substring(0, 25)}...");
                            }
                            Threads.TraceConsole($"Error in value field lenght in thread '{key}', expected:{expectedSize}, actual:{props["Value"].Length}, value is valid but from an other query: {valueIsValid}");
                        }

                        // if we have no OutOfMemory exception, we can add the value to results
                        results.Add(props["Value"]);
                        // generate a big string to consume memory and generate out of memory exceptions
                        // this string will be freed by GC so we can continue to run
                        bigString = string.Join("-", results);
                    }
                    catch (Exception ex)
                    {
                        exceptionStats.Add(ex);
                    }
                }
            });

            // Wait for all threads to end to avoid conflict in Exceptions stats dictionnary
            threads.WaitAll();

            Threads.TraceConsole("");
            Threads.TraceConsole("-------------------------------");
            exceptionStats.TraceStats();
            Threads.TraceConsole($"Total Rediscalls:{Threads.redisCallsCount}");
            Threads.TraceConsole($"Success redis calls:{Threads.redisCallsCount - corruptedDataCount - corruptedDataSizeCount - keyNotFoundCount}/{Threads.redisCallsCount}");
            Threads.TraceConsole($"Corrupted redis commands key value: {corruptedDataCount}");
            if (checkContent)
            {
                Threads.TraceConsole($"Corrupted redis commands data size: {corruptedDataSizeCount}");
            }
            Threads.TraceConsole($"Key not found unexpected errors {keyNotFoundCount}");
        }
    }
}

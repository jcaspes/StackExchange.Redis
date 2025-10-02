using System;
using System.Threading;
using StackExchange.Redis;

public class Program
{
    public static void Main(string[] args)
    {
        ConfigurationOptions configuration = new ConfigurationOptions()
        {
            EndPoints = { "localhost:6379" },
            DefaultDatabase = 1,
            AllowAdmin = true,
            AbortOnConnectFail = true,
            SyncTimeout = 5000,
        };

        ConnectionMultiplexer connection = ConnectionMultiplexer.Connect(configuration);

        if (!connection.IsConnected)
        {
            Console.WriteLine("No connection to redis, exiting");
            return;
        }

        IDatabase redisDb = connection.GetDatabase();
        redisDb.StringSet("key1", "value1");
        redisDb.StringSet("key2", "value2");

        // 2 exceptions are explicitly triggered during the read of key1:
        //  - The first occurs after the message being added in PhysicalConnection._writtenAwaitingResponse but
        //    before the message being written on the physical socket.
        //  - The second occurs during the catch block in WriteMessageToServerInsideWriteLock, preventing the
        //    connection to be teared down.
        //
        // The result is that:
        //  - StringGet("key1") ends in error and corrupts the queue.
        //  - StringGet("key3") adds its message to PhysicalConnection._writtenAwaitingResponse and writes it
        //    on the physical socket.
        //  - The redis answer ("key3 does not exist") is read and matched to the first message of the queue, "GET key1" (INCORRECT).
        //    AS key1 is already in error, the result is ignored.
        //  - StringGet("key2") (1 second later in another thread) adds its message to PhysicalConnection._writtenAwaitingResponse
        //    and writes it on the physical socket (normal).
        //  - The redis answer ("value2") is read and matched to the first message of the queue, "GET key3" (INCORRECT).
        try
        {
            string? value1 = redisDb.StringGet("key1");
            Console.WriteLine($"key1: {value1}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception during StringGet of key1: {ex.Message}");
        }

        Thread t = new Thread(() =>
        {
            Thread.Sleep(1000); // Ensure this runs after the method call StringGet("key3") below
            try
            {
                string? value2 = redisDb.StringGet("key2");
                Console.WriteLine($"key2 from thread: {value2}");
            }
            catch (RedisTimeoutException)
            {
                Console.WriteLine($"Timeout exception during StringGet of key2");
            }
        });
        t.Start();

        string? value3 = redisDb.StringGet("key3"); // Key does not exist, but it does not matter
        Console.WriteLine($"key3: {value3}");
        t.Join();
    }
}

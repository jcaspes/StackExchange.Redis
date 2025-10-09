using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace MemoryTesterWatchdog;

internal sealed class Program
{
    private static volatile bool _shouldExit = false;
    private static readonly string LogFileName = $"watchdog_log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

    // Variables for blockage detection
    private static int _consecutiveBlockedLogs = 0;
    private static readonly int MaxConsecutiveBlockedLogs = 10;

    // Variable to store current process reference
    private static Process? _currentProcess = null;
    private static readonly object _processLock = new object();

    public static async Task Main(string[] args)
    {
        Console.WriteLine("MemoryTesterWatchdog - Process Monitoring");
        Console.WriteLine("=========================================");

        // Configure Ctrl+C signal handler
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            _shouldExit = true;
            Console.WriteLine("\nShutdown requested. Closing...");

            // Kill child process immediately
            KillCurrentProcess();
        };

        // Default configuration - can be modified as needed
        List<string> parameters = new();
        foreach (int threadsCount in new[] { 1, 2, 4, 8, 16, 32, 64, 128, 256 })
        {
            foreach (int maxSize in new[] { 100, 1000, 10000, 100000, 1000000 })
            {
                for (int i = 0; i < 10; i++)
                {
                    parameters.Add($"nooom duration=10 threads={threadsCount} maxSize={maxSize}");
                    parameters.Add($"nooom duration=10 threads={threadsCount} maxSize={maxSize} HighIntegrity");
                }
            }
        }
        string processName = "cmd.exe";
        string processArguments = @"/c ""E:\DEV\StackExchange.Redis\toys\MemoryTester\bin\Debug\MemoryTester.exe"" ";

        Console.WriteLine($"Log file: {LogFileName}");
        Console.WriteLine($"Monitored process: {processName} {processArguments}");
        Console.WriteLine($"Blockage detection: {MaxConsecutiveBlockedLogs} consecutive logs without progress");
        Console.WriteLine("Press Ctrl+C to stop the watchdog\n");

        await WriteToLogAsync($"[{DateTime.Now}] Watchdog startup");

        foreach (string parameter in parameters)
        {
            try
            {
                _consecutiveBlockedLogs = 0;
                await RunProcessWithMonitoring(processName, processArguments + parameter);
            }
            catch (Exception ex)
            {
                var errorMessage = $"[{DateTime.Now}] ERROR: {ex.Message}";
                Console.WriteLine(errorMessage);
                await WriteToLogAsync(errorMessage);

                // Wait before restarting in case of error
                if (!_shouldExit)
                {
                    await Task.Delay(5000);
                }
            }

            if (!_shouldExit)
            {
                var restartMessage = $"[{DateTime.Now}] Restarting process in 1 seconds...";
                Console.WriteLine(restartMessage);
                await WriteToLogAsync(restartMessage);
                await Task.Delay(1000);
            }
        }

        await WriteToLogAsync($"[{DateTime.Now}] Watchdog shutdown");
        Console.WriteLine("Watchdog stopped.");
    }

    private static void KillCurrentProcess()
    {
        lock (_processLock)
        {
            if (_currentProcess != null && !_currentProcess.HasExited)
            {
                try
                {
                    _currentProcess.Kill(true); // true to also kill child processes
                    Console.WriteLine("Child process stopped successfully.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error stopping process: {ex.Message}");
                }
            }
        }
    }

    private static async Task RunProcessWithMonitoring(string processName, string arguments)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = processName,
            Arguments = arguments,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };

        using var process = new Process { StartInfo = startInfo };

        // Store current process reference
        lock (_processLock)
        {
            _currentProcess = process;
        }

        var startMessage = $"[{DateTime.Now}] Starting process: {processName} {arguments}";
        Console.WriteLine(startMessage);
        await WriteToLogAsync(startMessage);

        // Handlers to capture real-time output
        process.OutputDataReceived += async (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                var outputMessage = $"[{DateTime.Now}] STDOUT: {e.Data}";
                Console.WriteLine(outputMessage);
                await WriteToLogAsync(outputMessage);

                // Analyze log to detect blockage
                await AnalyzeLogForBlockage(e.Data, process);
            }
        };

        process.ErrorDataReceived += async (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                var errorMessage = $"[{DateTime.Now}] STDERR: {e.Data}";
                Console.WriteLine(errorMessage);
                await WriteToLogAsync(errorMessage);
            }
        };

        try
        {
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            // Wait for process to end or shutdown requested
            while (!process.HasExited && !_shouldExit)
            {
                await Task.Delay(100);
            }

            if (!process.HasExited)
            {
                // Forced shutdown requested
                var killMessage = $"[{DateTime.Now}] Forced process shutdown";
                Console.WriteLine(killMessage);
                await WriteToLogAsync(killMessage);

                try
                {
                    process.Kill(true); // true to also kill child processes
                    await process.WaitForExitAsync();
                }
                catch (Exception ex)
                {
                    var errorMessage = $"[{DateTime.Now}] Error during process shutdown: {ex.Message}";
                    Console.WriteLine(errorMessage);
                    await WriteToLogAsync(errorMessage);
                }
            }
            else
            {
                var exitMessage = $"[{DateTime.Now}] Process terminated with exit code: {process.ExitCode}";
                Console.WriteLine(exitMessage);
                await WriteToLogAsync(exitMessage);
            }
        }
        finally
        {
            // Clean up process reference
            lock (_processLock)
            {
                if (_currentProcess == process)
                {
                    _currentProcess = null;
                }
            }
        }
    }

    private static async Task AnalyzeLogForBlockage(string logLine, Process process)
    {
        try
        {
            // Pattern to detect MemoryTester logs
            // Example: "Main : ===> Threads loop, count: 186, redis calls: 50234, exceptions: 41156(+242), succeed calls: 137<==="
            var pattern = @"Main : ===> Threads loop, count: \d+, redis calls: (\d+)\(\+(\d+)\), exceptions: \d+\(\+\d+\), succeed calls: \d+<===";
            var match = Regex.Match(logLine, pattern);

            if (match.Success)
            {
                int currentCalls = int.Parse(match.Groups[1].Value);
                int newCalls = int.Parse(match.Groups[2].Value);

                // Blockage detection: Redis calls not progressing, 0 new exceptions, 0 successful calls
                bool isBlocked = currentCalls != 0 && newCalls == 0;
                if (isBlocked)
                {
                    _consecutiveBlockedLogs++;
                    var blockMessage = $"[{DateTime.Now}] BLOCKAGE DETECTION: Blocked log #{_consecutiveBlockedLogs}/{MaxConsecutiveBlockedLogs}";
                    Console.WriteLine(blockMessage);
                    await WriteToLogAsync(blockMessage);

                    if (_consecutiveBlockedLogs >= MaxConsecutiveBlockedLogs)
                    {
                        var killMessage = $"[{DateTime.Now}] BLOCKAGE DETECTED: {MaxConsecutiveBlockedLogs} consecutive logs without progress - Forcing MemoryTester shutdown";
                        Console.WriteLine(killMessage);

                        try
                        {
                            if (!process.HasExited)
                            {
                                process.Kill(true); // true to also kill child processes
                                await WriteToLogAsync($"[{DateTime.Now}] MemoryTester process killed successfully");
                            }
                        }
                        catch (Exception ex)
                        {
                            await WriteToLogAsync($"[{DateTime.Now}] Error during forced shutdown: {ex.Message}");
                        }
                    }
                }
                else
                {
                    // Reset counter if process is not blocked
                    if (_consecutiveBlockedLogs > 0)
                    {
                        var recoveryMessage = $"[{DateTime.Now}] RECOVERY: Process is progressing again - Redis calls: {currentCalls}";
                        Console.WriteLine(recoveryMessage);
                        await WriteToLogAsync(recoveryMessage);
                    }
                    _consecutiveBlockedLogs = 0;
                }
            }
        }
        catch (Exception ex)
        {
            await WriteToLogAsync($"[{DateTime.Now}] Error during log analysis: {ex.Message}");
        }
    }

    private static async Task WriteToLogAsync(string message)
    {
        try
        {
            await File.AppendAllTextAsync(LogFileName, message + Environment.NewLine, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error writing to log file: {ex.Message}");
        }
    }
}

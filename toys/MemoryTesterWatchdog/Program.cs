using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace MemoryTesterWatchdog;

internal sealed class Program
{
    private static volatile bool _shouldExit = false;
    private static readonly string LogFileName = $"watchdog_log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

    // Variables pour la détection de blocage
    private static int _consecutiveBlockedLogs = 0;
    private static readonly int MaxConsecutiveBlockedLogs = 10;

    // Variable pour stocker la référence du processus actuel
    private static Process? _currentProcess = null;
    private static readonly object _processLock = new object();

    public static async Task Main(string[] args)
    {
        Console.WriteLine("MemoryTesterWatchdog - Surveillance de processus");
        Console.WriteLine("=========================================");

        // Configuration du gestionnaire de signal Ctrl+C
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            _shouldExit = true;
            Console.WriteLine("\nArrêt demandé. Fermeture en cours...");

            // Tuer le processus enfant immédiatement
            KillCurrentProcess();
        };

        // Configuration par défaut - peut être modifiée selon les besoins
        string processName = "cmd.exe";
        string processArguments = @"/c ""E:\DEV\StackExchange.Redis\toys\MemoryTester\bin\Debug\MemoryTester.exe"" HighIntegrity";

        Console.WriteLine($"Fichier de log: {LogFileName}");
        Console.WriteLine($"Processus surveillé: {processName} {processArguments}");
        Console.WriteLine($"Détection de blocage: {MaxConsecutiveBlockedLogs} logs consécutifs sans progression");
        Console.WriteLine("Appuyez sur Ctrl+C pour arrêter le watchdog\n");

        await WriteToLogAsync($"[{DateTime.Now}] Démarrage du watchdog");

        while (!_shouldExit)
        {
            try
            {
                _consecutiveBlockedLogs = 0;
                await RunProcessWithMonitoring(processName, processArguments);
            }
            catch (Exception ex)
            {
                var errorMessage = $"[{DateTime.Now}] ERREUR: {ex.Message}";
                Console.WriteLine(errorMessage);
                await WriteToLogAsync(errorMessage);

                // Attendre avant de redémarrer en cas d'erreur
                if (!_shouldExit)
                {
                    await Task.Delay(5000);
                }
            }

            if (!_shouldExit)
            {
                var restartMessage = $"[{DateTime.Now}] Redémarrage du processus dans 2 secondes...";
                Console.WriteLine(restartMessage);
                await WriteToLogAsync(restartMessage);
                await Task.Delay(2000);
            }
        }

        await WriteToLogAsync($"[{DateTime.Now}] Arrêt du watchdog");
        Console.WriteLine("Watchdog arrêté.");
    }

    private static void KillCurrentProcess()
    {
        lock (_processLock)
        {
            if (_currentProcess != null && !_currentProcess.HasExited)
            {
                try
                {
                    _currentProcess.Kill(true); // true pour tuer aussi les processus enfants
                    Console.WriteLine("Processus enfant arrêté avec succès.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erreur lors de l'arrêt du processus: {ex.Message}");
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

        // Stocker la référence du processus actuel
        lock (_processLock)
        {
            _currentProcess = process;
        }

        var startMessage = $"[{DateTime.Now}] Démarrage du processus: {processName} {arguments}";
        Console.WriteLine(startMessage);
        await WriteToLogAsync(startMessage);

        // Gestionnaires pour capturer la sortie en temps réel
        process.OutputDataReceived += async (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                var outputMessage = $"[{DateTime.Now}] STDOUT: {e.Data}";
                Console.WriteLine(outputMessage);
                await WriteToLogAsync(outputMessage);

                // Analyser le log pour détecter un blocage
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

            // Attendre la fin du processus ou l'arrêt demandé
            while (!process.HasExited && !_shouldExit)
            {
                await Task.Delay(100);
            }

            if (!process.HasExited)
            {
                // Arrêt forcé demandé
                var killMessage = $"[{DateTime.Now}] Arrêt forcé du processus";
                Console.WriteLine(killMessage);
                await WriteToLogAsync(killMessage);

                try
                {
                    process.Kill(true); // true pour tuer aussi les processus enfants
                    await process.WaitForExitAsync();
                }
                catch (Exception ex)
                {
                    var errorMessage = $"[{DateTime.Now}] Erreur lors de l'arrêt du processus: {ex.Message}";
                    Console.WriteLine(errorMessage);
                    await WriteToLogAsync(errorMessage);
                }
            }
            else
            {
                var exitMessage = $"[{DateTime.Now}] Processus terminé avec le code de sortie: {process.ExitCode}";
                Console.WriteLine(exitMessage);
                await WriteToLogAsync(exitMessage);
            }
        }
        finally
        {
            // Nettoyer la référence du processus
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
            // Pattern pour détecter les logs de MemoryTester
            // Exemple: "Main : ===> Threads loop, count: 186, redis calls: 50234, exceptions: 41156(+242), succeed calls: 137<==="
            var pattern = @"Main : ===> Threads loop, count: \d+, redis calls: (\d+)\(\+(\d+)\), exceptions: \d+\(\+\d+\), succeed calls: \d+<===";
            var match = Regex.Match(logLine, pattern);

            if (match.Success)
            {
                int currentCalls = int.Parse(match.Groups[1].Value);
                int newCalls = int.Parse(match.Groups[2].Value);

                // Détection de blocage: Redis calls ne progressent pas, 0 nouvelles exceptions, 0 appels réussis
                bool isBlocked = currentCalls != 0 && newCalls == 0;
                if (isBlocked)
                {
                    _consecutiveBlockedLogs++;
                    var blockMessage = $"[{DateTime.Now}] DÉTECTION BLOCAGE: Log bloqué #{_consecutiveBlockedLogs}/{MaxConsecutiveBlockedLogs}";
                    Console.WriteLine(blockMessage);
                    await WriteToLogAsync(blockMessage);

                    if (_consecutiveBlockedLogs >= MaxConsecutiveBlockedLogs)
                    {
                        var killMessage = $"[{DateTime.Now}] BLOCAGE DÉTECTÉ: {MaxConsecutiveBlockedLogs} logs consécutifs sans progression - Arrêt forcé du processus MemoryTester";
                        Console.WriteLine(killMessage);

                        try
                        {
                            if (!process.HasExited)
                            {
                                process.Kill(true); // true pour tuer aussi les processus enfants
                                await WriteToLogAsync($"[{DateTime.Now}] Processus MemoryTester tué avec succès");
                            }
                        }
                        catch (Exception ex)
                        {
                            await WriteToLogAsync($"[{DateTime.Now}] Erreur lors de l'arrêt forcé: {ex.Message}");
                        }
                    }
                }
                else
                {
                    // Reset du compteur si le processus n'est pas bloqué
                    if (_consecutiveBlockedLogs > 0)
                    {
                        var recoveryMessage = $"[{DateTime.Now}] RÉCUPÉRATION: Le processus progresse à nouveau - Redis calls: {currentCalls}";
                        Console.WriteLine(recoveryMessage);
                        await WriteToLogAsync(recoveryMessage);
                    }
                    _consecutiveBlockedLogs = 0;
                }
            }
        }
        catch (Exception ex)
        {
            await WriteToLogAsync($"[{DateTime.Now}] Erreur lors de l'analyse du log: {ex.Message}");
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
            Console.WriteLine($"Erreur lors de l'écriture dans le fichier de log: {ex.Message}");
        }
    }
}

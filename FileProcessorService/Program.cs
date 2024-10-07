using FileProcessorService;
using System.Diagnostics;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

if (args.Length > 0)
{
    switch (args[0].ToLower())
    {
        case "--install":
            InstallService();
            break;
        case "--uninstall":
            UninstallService();
            break;
        case "--start":
            StartService();
            break;
        case "--stop":
            StopService();
            break;
        case "--console":
            ConsoleMode(host);
            break;
        default:
            Console.WriteLine("Ge�ersiz komut. Kullan�m: --install | --uninstall | --start | --stop | --console");
            break;
    }
}
else
{
    host.Run();
}

void InstallService()
{
    string serviceName = "FileProcessorService";
    string displayName = "File Processor Service";
    string executablePath = Process.GetCurrentProcess().MainModule.FileName;
    try
    {
        using (var process = new Process())
        {
            process.StartInfo.FileName = "sc.exe";
            process.StartInfo.Arguments = $"create {serviceName} binPath= \"{executablePath}\" DisplayName= \"{displayName}\" start= auto";
            process.StartInfo.Verb = "runas"; // Y�netici olarak �al��t�r
            process.Start();
            process.WaitForExit();
        }
        Console.WriteLine("Hizmet ba�ar�yla y�klendi.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Hizmet y�kleme hatas�: {ex.Message}");
    }
}

void UninstallService()
{
    string serviceName = "FileProcessorService";

    try
    {
        using (var process = new Process())
        {
            process.StartInfo.FileName = "sc.exe";
            process.StartInfo.Arguments = $"delete {serviceName}";
            process.StartInfo.Verb = "runas"; 
            process.Start();
            process.WaitForExit();
        }
        Console.WriteLine("Hizmet ba�ar�yla kald�r�ld�.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Hizmet kald�rma hatas�: {ex.Message}");
    }
}

void StartService()
{
    string serviceName = "FileProcessorService";

    try
    {
        using (var process = new Process())
        {
            process.StartInfo.FileName = "sc.exe";
            process.StartInfo.Arguments = $"start {serviceName}";
            process.StartInfo.Verb = "runas"; 
            process.Start();
            process.WaitForExit();
        }
        Console.WriteLine("Hizmet ba�ar�yla ba�lat�ld�.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Hizmet ba�latma hatas�: {ex.Message}");
    }
}

void StopService()
{
    string serviceName = "FileProcessorService";

    try
    {
        using (var process = new Process())
        {
            process.StartInfo.FileName = "sc.exe";
            process.StartInfo.Arguments = $"stop {serviceName}";
            process.StartInfo.Verb = "runas"; 
            process.Start();
            process.WaitForExit();
        }
        Console.WriteLine("Hizmet ba�ar�yla durduruldu.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Hizmet durdurma hatas�: {ex.Message}");
    }
}

void ConsoleMode(IHost host)
{
    Console.WriteLine("Konsol modunda �al���yor...");
    host.Run();
}

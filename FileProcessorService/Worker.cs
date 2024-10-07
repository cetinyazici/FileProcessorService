using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace FileProcessorService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger; 
        private readonly FileSystemWatcher _watcher; 
        private readonly string _path = @"C:\Users\Lenovo\Documents\MyDropFolder"; // Ýzlenecek klasörün yolu
        private readonly string _logFilePath = @"C:\Users\Lenovo\Documents\ProcessedLogs.json"; // Log dosyasýnýn yolu
        private readonly string _bootstrapServers; 
        private readonly string _topic;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger; 
            _watcher = new FileSystemWatcher(_path)
            {
                Filter = "*.evtx", // Sadece .evtx dosyalarýný izle
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime // Dosya adý ve oluþturma zamaný deðiþikliklerini izle
            };
            _watcher.Created += OnNewFileDetected; // Yeni dosya tespit edildiðinde olay
            _watcher.EnableRaisingEvents = true; // Olaylarý etkinleþtir
            _bootstrapServers = configuration["Kafka:BootstrapServers"]; // Kafka ayarlarýný yapýlandýr
            _topic = configuration["Kafka:Topic"]; // Kafka konusu ayarýný al
        }

        // Yeni dosya tespit edildiðinde tetiklenen metod
        public void OnNewFileDetected(object sender, FileSystemEventArgs e)
        {
            _logger.LogInformation($"New file detected: {e.FullPath}"); // Yeni dosya tespiti
            ProcessFile(e.FullPath); // Dosyayý iþle
        }

        // Dosyayý iþleme metod
        public void ProcessFile(string filePath)
        {
            var (jsonData, rowsParsed) = ParseEvtxFile(filePath); // EVTX dosyasýný JSON'a çevir
            SendToKafka(jsonData);                               // JSON verisini Kafka'ya gönder
            LogToFile(jsonData, filePath, rowsParsed);         // Parse edilen veriyi log dosyasýna yaz
        }

        // EVTX dosyasýný parse etme metod
        public (string jsonData, int rowsParsed) ParseEvtxFile(string filePath)
        {
            var rowsParsed = 100; // Ýþlenen satýr sayýsý (örnek olarak 100 alýndý)

            var parsedData = new
            {
                FileName = Path.GetFileName(filePath), // Dosya adýný al
                ProcessedAt = DateTime.Now, // Ýþleme zamaný
                RowsParsed = rowsParsed // Parse edilen satýr sayýsý
            };

            return (JsonSerializer.Serialize(parsedData), rowsParsed); // JSON verisini ve satýr sayýsýný döndür
        }

        // JSON verisini Kafka'ya gönderme metod
        public void SendToKafka(string jsonData)
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers }; // Kafka yapýlandýrmasý
            using (var producer = new ProducerBuilder<Null, string>(config).Build()) // Kafka üretici oluþtur
            {
                // Mesaj gönderme
                producer.Produce(_topic, new Message<Null, string> { Value = jsonData }, (deliveryReport) =>
                {
                    // Mesajýn durumu kontrolü
                    if (deliveryReport.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation($"Message sent to Kafka: {jsonData}"); // Baþarýlý gönderim
                    }
                    else
                    {
                        _logger.LogError($"Failed to send message to Kafka: {deliveryReport.Error.Reason}"); // Hata durumunda logla
                    }
                });
                producer.Flush(TimeSpan.FromSeconds(10)); // Zaman aþýmý kontrolü
            }
        }

        // Log dosyasýna yazma metod
        public void LogToFile(string jsonData, string filePath, int rowsParsed)
        {
            // Log dosyasýný kontrol et, yoksa oluþtur
            if (!File.Exists(_logFilePath))
            {
                using (File.Create(_logFilePath)) { }
            }

            // Log giriþi oluþtur
            var logEntry = new
            {
                FileName = Path.GetFileName(filePath), // Dosya adýný al
                ProcessedAt = DateTime.Now, // Ýþleme zamaný
                RowsParsed = rowsParsed, // Parse edilen satýr sayýsý
                JsonData = JsonSerializer.Deserialize<object>(jsonData) // JSON verisini deserialize et
            };

            // Log giriþini JSON formatýnda serialize et
            var logJson = JsonSerializer.Serialize(logEntry, new JsonSerializerOptions { WriteIndented = true });
            File.AppendAllText(_logFilePath, logJson + Environment.NewLine); // Log dosyasýna yaz

            _logger.LogInformation($"Parsed JSON data logged to {_logFilePath}"); // Log iþlemi tamamlandýðýnda bilgi ver
        }

        // Arka plan hizmetinin ana çalýþma metodu
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested) // Ýptal edilmediði sürece çalýþ
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now); // Çalýþma zamaný logu
                await Task.Delay(1000, stoppingToken); // Her 1 saniyede bir kontrol et
            }
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length > 0) // Komut satýrý argümanlarý varsa
            {
                switch (args[0].ToLower())
                {
                    case "-install":
                        InstallService(); // Servisi kur
                        break;
                    case "-uninstall":
                        UninstallService(); // Servisi kaldýr
                        break;
                    case "-start":
                        StartService(); // Servisi baþlat
                        break;
                    case "-stop":
                        StopService(); // Servisi durdur
                        break;
                    case "-console":
                        await RunAsConsole(); // Konsol modunda çalýþtýr
                        break;
                    default:
                        Console.WriteLine("Invalid argument. Use -install, -uninstall, -start, -stop, or -console."); // Geçersiz argüman durumu
                        break;
                }
            }
            else // Hiçbir argüman yoksa varsayýlan olarak çalýþtýr
            {
                await Host.CreateDefaultBuilder()
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddHostedService<Worker>(); // Worker servisini ekle
                    })
                    .ConfigureLogging(logging =>
                    {
                        logging.ClearProviders(); // Varsayýlan log saðlayýcýlarýný temizle
                        logging.AddConsole(); // Konsol log saðlayýcýsýný ekle
                    })
                    .RunConsoleAsync(); // Konsol modunda çalýþtýr
            }
        }

        // Konsol modunda çalýþtýrma metodu
        private static async Task RunAsConsole()
        {
            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>(); // Worker servisini ekle
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders(); // Varsayýlan log saðlayýcýlarýný temizle
                    logging.AddConsole(); // Konsol log saðlayýcýsýný ekle
                })
                .Build();

            Console.WriteLine("Running in console mode. Press Ctrl+C to exit."); // Konsol modunda çalýþtýðýný belirt
            await host.RunAsync(); // Asenkron olarak çalýþtýr
        }

        // Servisi kurma metodu
        private static void InstallService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "create FileProcessorService binPath= \"" + AppDomain.CurrentDomain.BaseDirectory + "FileProcessorService.exe\"") { Verb = "runas" });
            Console.WriteLine("Service installed."); // Servisin kurulduðunu belirt
        }

        // Servisi kaldýrma metodu
        private static void UninstallService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "delete FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service uninstalled."); // Servisin kaldýrýldýðýný belirt
        }

        // Servisi baþlatma metodu
        private static void StartService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "start FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service started."); // Servisin baþlatýldýðýný belirt
        }

        // Servisi durdurma metodu
        private static void StopService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "stop FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service stopped."); // Servisin durdurulduðunu belirt
        }
    }
}

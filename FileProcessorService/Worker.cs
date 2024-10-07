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
        private readonly string _path = @"C:\Users\Lenovo\Documents\MyDropFolder"; // �zlenecek klas�r�n yolu
        private readonly string _logFilePath = @"C:\Users\Lenovo\Documents\ProcessedLogs.json"; // Log dosyas�n�n yolu
        private readonly string _bootstrapServers; 
        private readonly string _topic;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger; 
            _watcher = new FileSystemWatcher(_path)
            {
                Filter = "*.evtx", // Sadece .evtx dosyalar�n� izle
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime // Dosya ad� ve olu�turma zaman� de�i�ikliklerini izle
            };
            _watcher.Created += OnNewFileDetected; // Yeni dosya tespit edildi�inde olay
            _watcher.EnableRaisingEvents = true; // Olaylar� etkinle�tir
            _bootstrapServers = configuration["Kafka:BootstrapServers"]; // Kafka ayarlar�n� yap�land�r
            _topic = configuration["Kafka:Topic"]; // Kafka konusu ayar�n� al
        }

        // Yeni dosya tespit edildi�inde tetiklenen metod
        public void OnNewFileDetected(object sender, FileSystemEventArgs e)
        {
            _logger.LogInformation($"New file detected: {e.FullPath}"); // Yeni dosya tespiti
            ProcessFile(e.FullPath); // Dosyay� i�le
        }

        // Dosyay� i�leme metod
        public void ProcessFile(string filePath)
        {
            var (jsonData, rowsParsed) = ParseEvtxFile(filePath); // EVTX dosyas�n� JSON'a �evir
            SendToKafka(jsonData);                               // JSON verisini Kafka'ya g�nder
            LogToFile(jsonData, filePath, rowsParsed);         // Parse edilen veriyi log dosyas�na yaz
        }

        // EVTX dosyas�n� parse etme metod
        public (string jsonData, int rowsParsed) ParseEvtxFile(string filePath)
        {
            var rowsParsed = 100; // ��lenen sat�r say�s� (�rnek olarak 100 al�nd�)

            var parsedData = new
            {
                FileName = Path.GetFileName(filePath), // Dosya ad�n� al
                ProcessedAt = DateTime.Now, // ��leme zaman�
                RowsParsed = rowsParsed // Parse edilen sat�r say�s�
            };

            return (JsonSerializer.Serialize(parsedData), rowsParsed); // JSON verisini ve sat�r say�s�n� d�nd�r
        }

        // JSON verisini Kafka'ya g�nderme metod
        public void SendToKafka(string jsonData)
        {
            var config = new ProducerConfig { BootstrapServers = _bootstrapServers }; // Kafka yap�land�rmas�
            using (var producer = new ProducerBuilder<Null, string>(config).Build()) // Kafka �retici olu�tur
            {
                // Mesaj g�nderme
                producer.Produce(_topic, new Message<Null, string> { Value = jsonData }, (deliveryReport) =>
                {
                    // Mesaj�n durumu kontrol�
                    if (deliveryReport.Status == PersistenceStatus.Persisted)
                    {
                        _logger.LogInformation($"Message sent to Kafka: {jsonData}"); // Ba�ar�l� g�nderim
                    }
                    else
                    {
                        _logger.LogError($"Failed to send message to Kafka: {deliveryReport.Error.Reason}"); // Hata durumunda logla
                    }
                });
                producer.Flush(TimeSpan.FromSeconds(10)); // Zaman a��m� kontrol�
            }
        }

        // Log dosyas�na yazma metod
        public void LogToFile(string jsonData, string filePath, int rowsParsed)
        {
            // Log dosyas�n� kontrol et, yoksa olu�tur
            if (!File.Exists(_logFilePath))
            {
                using (File.Create(_logFilePath)) { }
            }

            // Log giri�i olu�tur
            var logEntry = new
            {
                FileName = Path.GetFileName(filePath), // Dosya ad�n� al
                ProcessedAt = DateTime.Now, // ��leme zaman�
                RowsParsed = rowsParsed, // Parse edilen sat�r say�s�
                JsonData = JsonSerializer.Deserialize<object>(jsonData) // JSON verisini deserialize et
            };

            // Log giri�ini JSON format�nda serialize et
            var logJson = JsonSerializer.Serialize(logEntry, new JsonSerializerOptions { WriteIndented = true });
            File.AppendAllText(_logFilePath, logJson + Environment.NewLine); // Log dosyas�na yaz

            _logger.LogInformation($"Parsed JSON data logged to {_logFilePath}"); // Log i�lemi tamamland���nda bilgi ver
        }

        // Arka plan hizmetinin ana �al��ma metodu
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested) // �ptal edilmedi�i s�rece �al��
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now); // �al��ma zaman� logu
                await Task.Delay(1000, stoppingToken); // Her 1 saniyede bir kontrol et
            }
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length > 0) // Komut sat�r� arg�manlar� varsa
            {
                switch (args[0].ToLower())
                {
                    case "-install":
                        InstallService(); // Servisi kur
                        break;
                    case "-uninstall":
                        UninstallService(); // Servisi kald�r
                        break;
                    case "-start":
                        StartService(); // Servisi ba�lat
                        break;
                    case "-stop":
                        StopService(); // Servisi durdur
                        break;
                    case "-console":
                        await RunAsConsole(); // Konsol modunda �al��t�r
                        break;
                    default:
                        Console.WriteLine("Invalid argument. Use -install, -uninstall, -start, -stop, or -console."); // Ge�ersiz arg�man durumu
                        break;
                }
            }
            else // Hi�bir arg�man yoksa varsay�lan olarak �al��t�r
            {
                await Host.CreateDefaultBuilder()
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddHostedService<Worker>(); // Worker servisini ekle
                    })
                    .ConfigureLogging(logging =>
                    {
                        logging.ClearProviders(); // Varsay�lan log sa�lay�c�lar�n� temizle
                        logging.AddConsole(); // Konsol log sa�lay�c�s�n� ekle
                    })
                    .RunConsoleAsync(); // Konsol modunda �al��t�r
            }
        }

        // Konsol modunda �al��t�rma metodu
        private static async Task RunAsConsole()
        {
            var host = Host.CreateDefaultBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>(); // Worker servisini ekle
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders(); // Varsay�lan log sa�lay�c�lar�n� temizle
                    logging.AddConsole(); // Konsol log sa�lay�c�s�n� ekle
                })
                .Build();

            Console.WriteLine("Running in console mode. Press Ctrl+C to exit."); // Konsol modunda �al��t���n� belirt
            await host.RunAsync(); // Asenkron olarak �al��t�r
        }

        // Servisi kurma metodu
        private static void InstallService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "create FileProcessorService binPath= \"" + AppDomain.CurrentDomain.BaseDirectory + "FileProcessorService.exe\"") { Verb = "runas" });
            Console.WriteLine("Service installed."); // Servisin kuruldu�unu belirt
        }

        // Servisi kald�rma metodu
        private static void UninstallService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "delete FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service uninstalled."); // Servisin kald�r�ld���n� belirt
        }

        // Servisi ba�latma metodu
        private static void StartService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "start FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service started."); // Servisin ba�lat�ld���n� belirt
        }

        // Servisi durdurma metodu
        private static void StopService()
        {
            Process.Start(new ProcessStartInfo("sc.exe", "stop FileProcessorService") { Verb = "runas" });
            Console.WriteLine("Service stopped."); // Servisin durduruldu�unu belirt
        }
    }
}

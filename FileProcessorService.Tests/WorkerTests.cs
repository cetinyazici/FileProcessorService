using Moq; 
using Microsoft.Extensions.Logging; 
using Microsoft.Extensions.Configuration; 
using NUnit.Framework; 

namespace FileProcessorService.Tests
{
    public class WorkerTests
    {
        private Mock<ILogger<Worker>> _loggerMock; // Worker için sahte logger
        private Mock<IConfiguration> _configurationMock; // Worker için sahte konfigürasyon
        private Worker _worker; // Test edilen Worker nesnesi

        [SetUp]
        public void Setup()
        {
            // Testlerde kullanılacak sahte nesneleri oluştur
            _loggerMock = new Mock<ILogger<Worker>>(); // Sahte logger'ı başlat
            _configurationMock = new Mock<IConfiguration>(); // Sahte konfigürasyonu başlat

            // Konfigürasyon ayarlarını yapılandır
            _configurationMock.SetupGet(config => config["Kafka:BootstrapServers"]).Returns("localhost:9092");
            _configurationMock.SetupGet(config => config["Kafka:Topic"]).Returns("test-topic");

            // Worker nesnesini sahte logger ve konfigürasyon ile başlat
            _worker = new Worker(_loggerMock.Object, _configurationMock.Object);
        }

        [Test]
        public void ParseEvtxFile_ShouldReturnValidJsonDataAndRowCount()
        {
            // Test dosyasının yolu
            var testFilePath = @"C:\Users\Lenovo\Documents\security.evtx";
            // Dosyayı parse et ve JSON verisi ile satır sayısını al
            var (jsonData, rowsParsed) = _worker.ParseEvtxFile(testFilePath);

            // Sonuçları kontrol et
            Assert.That(jsonData, Is.Not.Null); // JSON verisi null olmamalı
            Assert.That(rowsParsed, Is.EqualTo(100)); // Parse edilen satır sayısı 100 olmalı
            Assert.That(jsonData, Does.Contain("security.evtx")); // JSON verisi dosya adını içermeli
        }

        [Test]
        public void SendToKafka_ShouldLogInformationOnSuccess()
        {
            var testJsonData = "{ \"key\": \"value\" }"; // Test için örnek JSON verisi
            _worker.SendToKafka(testJsonData); // Kafka'ya JSON verisini gönder

            // Log doğrulaması
            _loggerMock.Verify(logger => logger.Log(
                LogLevel.Information, // Log seviyesi
                It.IsAny<EventId>(), // Olay kimliği
                It.Is<string>(msg => msg.Contains("Message sent to Kafka")), // Log mesajı kontrolü
                null,
                It.IsAny<Func<string, Exception?, string>>() // Mesajın nasıl oluşturulduğu
                ),
            Times.Once, "Expected log message not found for sending to Kafka."); // Beklenen log mesajının bulunamaması durumu
        }

        [Test]
        public void LogToFile_ShouldWriteLogEntryToFile()
        {
            var testJsonData = "{ \"FileName\": \"security.evtx\" }"; // Test için JSON verisi
            var testFilePath = @"C:\Users\Lenovo\Documents\ProcessedLogs.json"; // Log dosyasının yolu
            _worker.LogToFile(testJsonData, testFilePath, 100); // Log girişini dosyaya yaz

            // Log doğrulaması
            _loggerMock.Verify(logger => logger.Log(
                LogLevel.Information, // Log seviyesi
                It.IsAny<EventId>(), // Olay kimliği
                It.Is<string>(msg => msg.Contains("Parsed JSON data logged to")), // Log mesajı kontrolü
                null,
                It.IsAny<Func<string, Exception?, string>>() // Mesajın nasıl oluşturulduğu
                ),
            Times.Once, "Expected log message not found for logging to file."); // Beklenen log mesajının bulunamaması durumu
        }

    }
}

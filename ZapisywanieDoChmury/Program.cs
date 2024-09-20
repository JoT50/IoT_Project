using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.UaFx.Client;
using Opc.UaFx;
using System.Text;

class Program
{
    private static string deviceConnectionString = "HostName=Zajecia02.azure-devices.net;DeviceId=test_device;SharedAccessKey=cUnZn05tTHr6DL/OsBVFPMUFVBIneTopQAIoTE4fqRc=";

    static async Task Main(string[] args)
    {
        using (var client = new OpcClient("opc.tcp://localhost:4840/"))
        {
            client.Connect();

            OpcReadNode[] commands = new OpcReadNode[] {
                new OpcReadNode("ns=2;s=Device 1/ProductionStatus", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/ProductionStatus"), // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/ProductionRate", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/ProductionRate"),   // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/WorkorderId", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/WorkorderId"),      // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/Temperature", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/Temperature"),      // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/GoodCount", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/GoodCount"),        // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/BadCount", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/BadCount"),         // Odczyt wartości
                new OpcReadNode("ns=2;s=Device 1/DeviceError", OpcAttribute.DisplayName),
                new OpcReadNode("ns=2;s=Device 1/DeviceError"),      // Odczyt wartości
            };

            IEnumerable<OpcValue> values = client.ReadNodes(commands);

            // Wypisywanie wartości z serwera OPC
            foreach (var item in values)
            {
                Console.WriteLine(item.Value);
            }

            // Przygotowanie i wysyłka telemetrii (ProductionStatus, WorkorderId, Temperature, GoodCount, BadCount)
            var telemetryData = new
            {
                ProductionStatus = values.ElementAt(1).Value,
                WorkorderId = values.ElementAt(5).Value,
                Temperature = values.ElementAt(7).Value,
                GoodCount = values.ElementAt(9).Value,
                BadCount = values.ElementAt(11).Value
            };

            await SendMessageToIoTHub(telemetryData);

            // Przypisanie wartości do zmiennych
            var productionStatus = Convert.ToInt32(values.ElementAt(1).Value); // ProductionStatus
            var productionRate = Convert.ToInt32(values.ElementAt(3).Value);   // ProductionRate
            var workorderId = values.ElementAt(5).Value.ToString();            // WorkorderId
            var temperature = Convert.ToDouble(values.ElementAt(7).Value);     // Temperature
            var goodCount = Convert.ToInt32(values.ElementAt(9).Value);        // GoodCount
            var badCount = Convert.ToInt32(values.ElementAt(11).Value);        // BadCount
            var deviceErrorCode = Convert.ToInt32(values.ElementAt(13).Value); // DeviceError

            // Wypisanie wartości na konsolę
            Console.WriteLine("ProductionStatus: " + productionStatus);
            Console.WriteLine("ProductionRate: " + productionRate);
            Console.WriteLine("WorkorderId: " + workorderId);
            Console.WriteLine("Temperature: " + temperature);
            Console.WriteLine("GoodCount: " + goodCount);
            Console.WriteLine("BadCount: " + badCount);
            Console.WriteLine("DeviceError: " + deviceErrorCode);

            // Pobranie wiadomości błędu na podstawie kodu błędu
            string errorMessage = GetDeviceErrorMessage(deviceErrorCode);
            Console.WriteLine("Numer błędu: " + deviceErrorCode + ", Komunikat: " + errorMessage);

            // Sprawdzenie poprzedniego DeviceError z Device Twin
            int reportedDeviceError = await GetReportedDeviceErrorAsync();
            Console.WriteLine("Reported DeviceError z Device Twin: " + reportedDeviceError);

            // Jeśli DeviceError się zmienił, wysyłamy wiadomość D2C i aktualizujemy Twin
            if (deviceErrorCode != reportedDeviceError)
            {
                Console.WriteLine($"DeviceError changed from {reportedDeviceError} to {deviceErrorCode}");

                var deviceError = new
                {
                    DeviceId = "Device 1",
                    ErrorCode = deviceErrorCode,
                    ErrorMessage = errorMessage,
                    Timestamp = DateTime.UtcNow
                 };

                    await SendDeviceErrorEventToIoTHub(deviceError);
                    await UpdateReportedDeviceErrorAsync(deviceErrorCode, errorMessage); // Aktualizacja Device Twin
            }
            else
            {
                Console.WriteLine("DeviceError unchanged.");
            }

            // Monitorowanie zmian Device Twin
            await MonitorDeviceTwinChangesAsync(client);
        }
    }

    // Funkcja do wysyłania danych telemetrycznych do IoT Hub
    private static async Task SendMessageToIoTHub(object telemetryData)
    {
        var messageString = JsonConvert.SerializeObject(telemetryData);
        var message = new Message(Encoding.ASCII.GetBytes(messageString));

        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            await deviceClient.SendEventAsync(message);
            Console.WriteLine("Telemetry message sent to IoT Hub");
        }
    }

    // Funkcja do generowania wiadomości błędu na podstawie kodu
    private static string GetDeviceErrorMessage(int errorCode)
    {
        string errorMessage = "None";

        switch (errorCode)
        {
            case 1:
                errorMessage = "Emergency Stop";
                break;
            case 2:
                errorMessage = "Power Failure";
                break;
            case 4:
                errorMessage = "Sensor Failure";
                break;
            case 8:
                errorMessage = "Unknown Error";
                break;
            default:
                if (errorCode > 0)
                {
                    errorMessage = "Multiple Errors";
                }
                break;
        }

        return errorMessage;
    }

    // Funkcja do pobrania poprzedniego DeviceError z Twin
    private static async Task<int> GetReportedDeviceErrorAsync()
    {
        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            Twin twin = await deviceClient.GetTwinAsync();
            if (twin.Properties.Reported.Contains("DeviceError"))
            {
                return (int)twin.Properties.Reported["DeviceError"];
            }
            return 0;
        }
    }

    // Funkcja do wysyłania zdarzenia DeviceError do IoT Hub
    private static async Task SendDeviceErrorEventToIoTHub(object deviceError)
    {
        var errorMessageString = JsonConvert.SerializeObject(deviceError);
        var message = new Message(Encoding.ASCII.GetBytes(errorMessageString));

        message.Properties.Add("MessageType", "DeviceError");
        message.Properties.Add("EventType", "Error");

        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            await deviceClient.SendEventAsync(message);
            Console.WriteLine("DeviceError event sent to IoT Hub");
        }
    }

    // Funkcja do aktualizacji wartości DeviceError w Device Twin
    private static async Task UpdateReportedDeviceErrorAsync(int errorCode, string errorMessage)
    {
        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            var reportedProperties = new TwinCollection();
            reportedProperties["DeviceError"] = errorCode;

            await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
            Console.WriteLine($"Updated Reported Device Error: {errorCode}, {errorMessage}");
        }
    }

    // Monitorowanie zmian w Device Twin
    private static async Task MonitorDeviceTwinChangesAsync(OpcClient opcClient)
    {
        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            await deviceClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChanged, opcClient);
        }
    }

    // Obsługa zmian w Device Twin dla ProductionRate
    private static async Task OnDesiredPropertyChanged(TwinCollection desiredProperties, object userContext)
    {
        OpcClient opcClient = (OpcClient)userContext;

        if (desiredProperties.Contains("ProductionRate"))
        {
            int desiredProductionRate = desiredProperties["ProductionRate"];
            Console.WriteLine($"Desired Production Rate: {desiredProductionRate}%");

            // Aktualizacja ProductionRate w serwerze OPC
            opcClient.WriteNode("ns=2;s=Device 1/ProductionRate", desiredProductionRate);
            await UpdateReportedProductionRateAsync(desiredProductionRate);
        }
    }

    // Funkcja do aktualizacji wartości ProductionRate w Device Twin
    private static async Task UpdateReportedProductionRateAsync(int currentProductionRate)
    {
        using (DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt))
        {
            var reportedProperties = new TwinCollection();
            reportedProperties["ProductionRate"] = currentProductionRate;

            await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
            Console.WriteLine($"Updated Reported Production Rate: {currentProductionRate}%");
        }
    }
}

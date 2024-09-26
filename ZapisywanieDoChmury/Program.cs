using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.UaFx.Client;
using Opc.UaFx;
using System.Text;

class Program
{
    private static Dictionary<string, string> deviceConnectionStrings = new Dictionary<string, string>();
    private static Dictionary<string, DeviceClient> deviceClients = new Dictionary<string, DeviceClient>();
    private static OpcClient opcClient;

    static async Task Main(string[] args)
    {
        string filePath = "plik.txt";
        string[] lines = File.ReadAllLines(filePath);

        // Wczytanie adresu OPC UA serwera (pierwsza linia)
        string opcServerAddress = lines[0];
        Console.WriteLine($"OPC UA Server Address: {opcServerAddress}\n");

        Console.WriteLine("Connecting Devices...\n");

        foreach (string line in lines.Skip(1))
        {
            Console.WriteLine(line);
            var parts = line.Split(',');
            if (parts.Length == 2)
            {
                string deviceId = parts[0].Trim();
                string connectionString = parts[1].Trim();
                deviceConnectionStrings.Add(deviceId, connectionString);
            }
        }

        opcClient = new OpcClient(opcServerAddress);
        opcClient.Connect();

        Console.WriteLine("\nMonitoring Device Twin changes...\n");

        // Inicjalizacja klienta dla każdego urządzenia
        foreach (var deviceId in deviceConnectionStrings.Keys)
        {
            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionStrings[deviceId], TransportType.Mqtt);
            deviceClients.Add(deviceId, deviceClient);

            await deviceClient.SetMethodHandlerAsync("EmergencyStop", EmergencyStopMethodHandler, deviceId);
            await deviceClient.SetMethodHandlerAsync("ResetErrorStatus", ResetErrorStatusMethodHandler, deviceId);

            await MonitorDeviceTwinChangesAsync(deviceId);
        }

        Console.WriteLine("Devices Connected.\n");

        while (true)
        {
            foreach (var deviceId in deviceClients.Keys)
            {
                try
                {
                    OpcReadNode[] commands = new OpcReadNode[] {
                        new OpcReadNode($"ns=2;s={deviceId}/ProductionStatus", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/ProductionStatus"),
                        new OpcReadNode($"ns=2;s={deviceId}/ProductionRate", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/ProductionRate"),
                        new OpcReadNode($"ns=2;s={deviceId}/WorkorderId", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/WorkorderId"),
                        new OpcReadNode($"ns=2;s={deviceId}/Temperature", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/Temperature"),
                        new OpcReadNode($"ns=2;s={deviceId}/GoodCount", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/GoodCount"),
                        new OpcReadNode($"ns=2;s={deviceId}/BadCount", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/BadCount"),
                        new OpcReadNode($"ns=2;s={deviceId}/DeviceError", OpcAttribute.DisplayName),
                        new OpcReadNode($"ns=2;s={deviceId}/DeviceError"),
                    };

                    IEnumerable<OpcValue> values = opcClient.ReadNodes(commands);

                    var telemetryData = new
                    {
                        DeviceId = deviceId,
                        ProductionStatus = values.ElementAt(1).Value,
                        WorkorderId = values.ElementAt(5).Value,
                        Temperature = values.ElementAt(7).Value,
                        GoodCount = values.ElementAt(9).Value,
                        BadCount = values.ElementAt(11).Value
                    };

                    await SendMessageToIoTHub(deviceId, telemetryData);

                    var productionStatus = Convert.ToInt32(values.ElementAt(1).Value);
                    var productionRate = Convert.ToInt32(values.ElementAt(3).Value);
                    var workorderId = values.ElementAt(5).Value.ToString();
                    var temperature = Convert.ToDouble(values.ElementAt(7).Value);
                    var goodCount = Convert.ToInt32(values.ElementAt(9).Value);
                    var badCount = Convert.ToInt32(values.ElementAt(11).Value);
                    var deviceErrorCode = Convert.ToInt32(values.ElementAt(13).Value);

                    Console.WriteLine($"DeviceId: {deviceId}");
                    Console.WriteLine($"ProductionStatus: {productionStatus}");
                    Console.WriteLine($"ProductionRate: {productionRate}");
                    Console.WriteLine($"WorkorderId: {workorderId}");
                    Console.WriteLine($"Temperature: {temperature}");
                    Console.WriteLine($"GoodCount: {goodCount}");
                    Console.WriteLine($"BadCount: {badCount}");
                    Console.WriteLine($"DeviceError: {deviceErrorCode}");

                    string errorMessage = GetDeviceErrorMessage(deviceErrorCode);
                    Console.WriteLine($"Error Number: {deviceErrorCode}, Message: {errorMessage}");

                    int reportedDeviceError = await GetReportedDeviceErrorAsync(deviceId);
                    Console.WriteLine($"Reported DeviceError from Device Twin: {reportedDeviceError}");

                    if (deviceErrorCode != reportedDeviceError && deviceErrorCode != 0)
                    {
                        // Jeśli błąd się zmienił i nie jest równy 0, wysyłamy event do IoT Hub oraz aktualizujemy Twin
                        Console.WriteLine($"DeviceError changed from {reportedDeviceError} to {deviceErrorCode}");

                        var deviceError = new
                        {
                            DeviceId = deviceId,
                            ErrorCode = deviceErrorCode,
                            ErrorMessage = errorMessage,
                            Timestamp = DateTime.UtcNow
                        };

                        // Wysyłanie wiadomości o błędzie do IoT Hub
                        await SendDeviceErrorEventToIoTHub(deviceId, deviceError);

                        // Aktualizacja Twin (reported properties)
                        await UpdateReportedDeviceErrorAsync(deviceId, deviceErrorCode, errorMessage);
                    }
                    else if (deviceErrorCode == 0 && reportedDeviceError != 0)
                    {
                        // Jeśli błąd zmienia się na 0, tylko aktualizujemy Twin bez wysyłania wiadomości
                        await UpdateReportedDeviceErrorAsync(deviceId, deviceErrorCode, errorMessage);
                        Console.WriteLine($"DeviceError cleared (changed from {reportedDeviceError} to {deviceErrorCode})");
                    }
                    else
                    {
                        // Wartość błędu się nie zmieniła
                        Console.WriteLine("DeviceError unchanged.");
                    }


                    Console.WriteLine("");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error occurred: {ex.Message}");
                }
            }
            await Task.Delay(7000);
        }
    }

    private static async Task MonitorDeviceTwinChangesAsync(string deviceId)
    {
        await deviceClients[deviceId].SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChanged, deviceId);
    }

    private static async Task OnDesiredPropertyChanged(TwinCollection desiredProperties, object userContext)
    {
        string deviceId = (string)userContext;

        if (desiredProperties.Contains("ProductionRate"))
        {
            int desiredProductionRate = desiredProperties["ProductionRate"];
            string currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            Console.WriteLine($"[TIME: {currentTime}] Desired Production Rate received for {deviceId}: {desiredProductionRate}%");

            try
            {
                ChangeProductionRate(deviceId, desiredProductionRate);
                Console.WriteLine($"[TIME: {currentTime}] Production Rate updated for {deviceId} to: {desiredProductionRate}%");

                await UpdateReportedProductionRateAsync(deviceId, desiredProductionRate);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TIME: {currentTime}] Error updating Production Rate for {deviceId}: {ex.Message}");
            }
        }
    }

    private static void ChangeProductionRate(string deviceId, int newProductionRate)
    {
        string productionRateNodeId = $"ns=2;s={deviceId}/ProductionRate";
        opcClient.WriteNode(productionRateNodeId, newProductionRate);
        Console.WriteLine($"Production Rate changed to: {newProductionRate}% for {deviceId}");
    }

    private static async Task UpdateReportedProductionRateAsync(string deviceId, int currentProductionRate)
    {
        var reportedProperties = new TwinCollection();
        reportedProperties["ProductionRate"] = currentProductionRate;

        await deviceClients[deviceId].UpdateReportedPropertiesAsync(reportedProperties);
        Console.WriteLine($"Reported Production Rate updated to: {currentProductionRate}% for {deviceId}");
    }

    private static async Task SendMessageToIoTHub(string deviceId, object telemetryData)
    {
        var messageString = JsonConvert.SerializeObject(telemetryData);
        var message = new Message(Encoding.ASCII.GetBytes(messageString));

        message.Properties.Add("MessageType", "Telemetry");

        await deviceClients[deviceId].SendEventAsync(message);
        Console.WriteLine($"Telemetry message sent to IoT Hub for {deviceId}");
    }

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

    private static async Task<int> GetReportedDeviceErrorAsync(string deviceId)
    {
        Twin twin = await deviceClients[deviceId].GetTwinAsync();
        if (twin.Properties.Reported.Contains("DeviceError"))
        {
            return (int)twin.Properties.Reported["DeviceError"];
        }
        return 0;
    }

    private static async Task SendDeviceErrorEventToIoTHub(string deviceId, object deviceError)
    {
        var messageString = JsonConvert.SerializeObject(deviceError);
        var message = new Message(Encoding.ASCII.GetBytes(messageString));

        message.Properties.Add("MessageType", "DeviceError");

        await deviceClients[deviceId].SendEventAsync(message);
        Console.WriteLine($"DeviceError event sent to IoT Hub for {deviceId}");
    }

    private static async Task UpdateReportedDeviceErrorAsync(string deviceId, int errorCode, string errorMessage)
    {
        var reportedProperties = new TwinCollection();
        reportedProperties["DeviceError"] = errorCode;
        reportedProperties["ErrorMessage"] = errorMessage;

        await deviceClients[deviceId].UpdateReportedPropertiesAsync(reportedProperties);
        Console.WriteLine($"Reported DeviceError updated to: {errorCode} ({errorMessage}) for {deviceId}");
    }

    private static async Task<MethodResponse> EmergencyStopMethodHandler(MethodRequest methodRequest, object userContext)
    {
        string deviceId = (string)userContext; // Get deviceId from userContext
        Console.WriteLine($"Emergency Stop triggered for device {deviceId}!");

        try
        {
            CallEmergencyStop(deviceId);
            string result = "{\"result\":\"Executed Emergency Stop\"}";
            return new MethodResponse(Encoding.UTF8.GetBytes(result), 200); // 200: Success
        }
        catch (Exception ex)
        {
            string result = $"{{\"error\":\"{ex.Message}\"}}";
            Console.WriteLine($"Error executing Emergency Stop for device {deviceId}: {ex.Message}");
            return new MethodResponse(Encoding.UTF8.GetBytes(result), 500); // 500: Error
        }
    }

    private static async Task<MethodResponse> ResetErrorStatusMethodHandler(MethodRequest methodRequest, object userContext)
    {
        string deviceId = (string)userContext; // Get deviceId from userContext
        Console.WriteLine($"Reset Error Status triggered for device {deviceId}!");

        try
        {
            ResetErrorStatus(deviceId);
            string result = "{\"result\":\"Executed Reset Error Status\"}";
            return new MethodResponse(Encoding.UTF8.GetBytes(result), 200); // 200: Success
        }
        catch (Exception ex)
        {
            string result = $"{{\"error\":\"{ex.Message}\"}}";
            Console.WriteLine($"Error executing Reset Error Status for device {deviceId}: {ex.Message}");
            return new MethodResponse(Encoding.UTF8.GetBytes(result), 500); // 500: Error
        }
    }


    public static void CallEmergencyStop(string deviceId)
    {
        try
        {
            string objectNodeId = $"ns=2;s={deviceId}"; // Object node ID (device node)
            string methodNodeId = $"ns=2;s={deviceId}/EmergencyStop"; // Method node ID for EmergencyStop
            Console.WriteLine($"Calling Emergency Stop method on node: {methodNodeId}");

            var result = opcClient.CallMethod(objectNodeId, methodNodeId); // Pass both object and method IDs
            Console.WriteLine($"Emergency stop completed for {deviceId}. Result: {result}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error calling Emergency Stop for {deviceId}: {ex.Message}");
        }
    }



    public static void ResetErrorStatus(string deviceId)
    {
        try
        {
            // Define the object node ID (Device node)
            string objectNodeId = $"ns=2;s={deviceId}";  // Should be "Device 1"
                                                         // Define the method node ID for ResetErrorStatus
            string methodNodeId = $"ns=2;s={deviceId}/ResetErrorStatus";  // Should be "Device 1/ResetErrorStatus"

            Console.WriteLine($"Calling Reset Error Status method on node: {methodNodeId}");

            // Prepare input arguments (if needed, pass an empty list if no parameters are required)
            IList<object> inputArguments = new List<object>();  // Typically empty unless specified by your method

            // Call the method on the OPC UA client
            var result = opcClient.CallMethod(objectNodeId, methodNodeId, inputArguments.ToArray());

            // Handle result or response if applicable
            Console.WriteLine($"Reset Error Status completed for {deviceId}. Result: {result}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error resetting error status for {deviceId}: {ex.Message}");
        }
    }




}

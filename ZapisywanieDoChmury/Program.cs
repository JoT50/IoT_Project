using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.UaFx.Client;
using Opc.UaFx;
using System.Text;

class Program
{
    // Connection strings dla urządzeń
    private static Dictionary<string, string> deviceConnectionStrings = new Dictionary<string, string>
    {
        { "Device 1", "HostName=Zajecia02.azure-devices.net;DeviceId=test_device;SharedAccessKey=cUnZn05tTHr6DL/OsBVFPMUFVBIneTopQAIoTE4fqRc=" },
        { "Device 2", "HostName=Zajecia02.azure-devices.net;DeviceId=test_device2;SharedAccessKey=D0aBqzMTsu0xfLVHLP2repPkA3S1ftVB66nXW/T1T1E=" }
    };

    private static Dictionary<string, DeviceClient> deviceClients = new Dictionary<string, DeviceClient>();
    private static OpcClient opcClient;

    static async Task Main(string[] args)
    {
        // Inicjalizacja klienta OPC UA
        opcClient = new OpcClient("opc.tcp://localhost:4840/");
        opcClient.Connect();

        Console.WriteLine("Monitoring Device Twin changes...\n");

        // Inicjalizacja klienta IoT dla każdego urządzenia
        foreach (var deviceId in deviceConnectionStrings.Keys)
        {
            var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionStrings[deviceId], TransportType.Mqtt);
            deviceClients.Add(deviceId, deviceClient);

            await deviceClient.SetMethodHandlerAsync("EmergencyStop", EmergencyStopMethodHandler, deviceId);
            await deviceClient.SetMethodHandlerAsync("ResetErrorStatus", ResetErrorStatusMethodHandler, deviceId);

            await MonitorDeviceTwinChangesAsync(deviceId);
        }

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
                        Console.WriteLine($"DeviceError changed from {reportedDeviceError} to {deviceErrorCode}");

                        var deviceError = new
                        {
                            DeviceId = deviceId,
                            ErrorCode = deviceErrorCode,
                            ErrorMessage = errorMessage,
                            Timestamp = DateTime.UtcNow
                        };

                        await SendDeviceErrorEventToIoTHub(deviceId, deviceError);
                        await UpdateReportedDeviceErrorAsync(deviceId, deviceErrorCode, errorMessage);
                    }
                    else
                    {
                        Console.WriteLine("DeviceError unchanged.");
                    }

                    Console.WriteLine("");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error occurred: {ex.Message}");
                }
            }
            await Task.Delay(3000);
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
        var errorMessageString = JsonConvert.SerializeObject(deviceError);
        var message = new Message(Encoding.UTF8.GetBytes(errorMessageString));

        message.Properties.Add("MessageType", "DeviceError");
        message.Properties.Add("EventType", "Error");
        message.ContentType = "application/json";
        message.ContentEncoding = "utf-8";

        await deviceClients[deviceId].SendEventAsync(message);
        Console.WriteLine($"Device error event sent to IoT Hub for {deviceId}");
    }

    private static async Task UpdateReportedDeviceErrorAsync(string deviceId, int errorCode, string errorMessage)
    {
        var reportedProperties = new TwinCollection();
        reportedProperties["DeviceError"] = errorCode;
        reportedProperties["DeviceErrorMessage"] = errorMessage;

        await deviceClients[deviceId].UpdateReportedPropertiesAsync(reportedProperties);
        Console.WriteLine($"DeviceError reported as {errorCode}: {errorMessage} for {deviceId}");
    }

    private static Task<MethodResponse> EmergencyStopMethodHandler(MethodRequest methodRequest, object userContext)
    {
        string deviceId = (string)userContext;
        Console.WriteLine($"Emergency stop triggered for {deviceId}");

        return Task.FromResult(new MethodResponse(200));
    }

    private static Task<MethodResponse> ResetErrorStatusMethodHandler(MethodRequest methodRequest, object userContext)
    {
        string deviceId = (string)userContext;
        Console.WriteLine($"Resetting error status for {deviceId}");

        return Task.FromResult(new MethodResponse(200));
    }
}

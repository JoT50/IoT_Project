using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using Opc.UaFx.Client;
using Opc.UaFx;
using System.Text;

class Program
{
    private static string deviceConnectionString = "HostName=Zajecia02.azure-devices.net;DeviceId=test_device;SharedAccessKey=cUnZn05tTHr6DL/OsBVFPMUFVBIneTopQAIoTE4fqRc=";
    private static DeviceClient deviceClient;
    private static OpcClient opcClient;

    static async Task Main(string[] args)
    {
        deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Mqtt);
        opcClient = new OpcClient("opc.tcp://localhost:4840/");
        opcClient.Connect();

        Console.WriteLine("Monitoring Device Twin changes...\n");
        await MonitorDeviceTwinChangesAsync();

        while (true)
        {
            try
            {
                OpcReadNode[] commands = new OpcReadNode[] {
                    new OpcReadNode("ns=2;s=Device 1/ProductionStatus", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/ProductionStatus"),
                    new OpcReadNode("ns=2;s=Device 1/ProductionRate", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/ProductionRate"),
                    new OpcReadNode("ns=2;s=Device 1/WorkorderId", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/WorkorderId"),
                    new OpcReadNode("ns=2;s=Device 1/Temperature", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/Temperature"),
                    new OpcReadNode("ns=2;s=Device 1/GoodCount", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/GoodCount"),
                    new OpcReadNode("ns=2;s=Device 1/BadCount", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/BadCount"),
                    new OpcReadNode("ns=2;s=Device 1/DeviceError", OpcAttribute.DisplayName),
                    new OpcReadNode("ns=2;s=Device 1/DeviceError"),
                };

                IEnumerable<OpcValue> values = opcClient.ReadNodes(commands);

                var telemetryData = new
                {
                    ProductionStatus = values.ElementAt(1).Value,
                    WorkorderId = values.ElementAt(5).Value,
                    Temperature = values.ElementAt(7).Value,
                    GoodCount = values.ElementAt(9).Value,
                    BadCount = values.ElementAt(11).Value
                };

                await SendMessageToIoTHub(telemetryData);

                var productionStatus = Convert.ToInt32(values.ElementAt(1).Value);
                var productionRate = Convert.ToInt32(values.ElementAt(3).Value);
                var workorderId = values.ElementAt(5).Value.ToString();
                var temperature = Convert.ToDouble(values.ElementAt(7).Value);
                var goodCount = Convert.ToInt32(values.ElementAt(9).Value);
                var badCount = Convert.ToInt32(values.ElementAt(11).Value);
                var deviceErrorCode = Convert.ToInt32(values.ElementAt(13).Value);

                Console.WriteLine($"ProductionStatus: {productionStatus}");
                Console.WriteLine($"ProductionRate: {productionRate}");
                Console.WriteLine($"WorkorderId: {workorderId}");
                Console.WriteLine($"Temperature: {temperature}");
                Console.WriteLine($"GoodCount: {goodCount}");
                Console.WriteLine($"BadCount: {badCount}");
                Console.WriteLine($"DeviceError: {deviceErrorCode}");

                string errorMessage = GetDeviceErrorMessage(deviceErrorCode);
                Console.WriteLine($"Error Number: {deviceErrorCode}, Message: {errorMessage}");

                int reportedDeviceError = await GetReportedDeviceErrorAsync();
                Console.WriteLine($"Reported DeviceError from Device Twin: {reportedDeviceError}");

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
                    await UpdateReportedDeviceErrorAsync(deviceErrorCode, errorMessage);
                }
                else
                {
                    Console.WriteLine("DeviceError unchanged.");
                }

                Console.WriteLine("");
                await Task.Delay(5000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error occurred: {ex.Message}");
            }
        }
    }

    private static async Task MonitorDeviceTwinChangesAsync()
    {
        await deviceClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChanged, null);
    }

    private static async Task OnDesiredPropertyChanged(TwinCollection desiredProperties, object userContext)
    {
        if (desiredProperties.Contains("ProductionRate"))
        {
            int desiredProductionRate = desiredProperties["ProductionRate"];
            string currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            Console.WriteLine($"[TIME: {currentTime}] Desired Production Rate received: {desiredProductionRate}%");

            try
            {
                ChangeProductionRate(desiredProductionRate);
                Console.WriteLine($"[TIME: {currentTime}] Production Rate updated to: {desiredProductionRate}%");

                await UpdateReportedProductionRateAsync(desiredProductionRate);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TIME: {currentTime}] Error updating Production Rate: {ex.Message}");
            }
        }
    }

    private static void ChangeProductionRate(int newProductionRate)
    {
        string productionRateNodeId = "ns=2;s=Device 1/ProductionRate";
        opcClient.WriteNode(productionRateNodeId, newProductionRate);
        Console.WriteLine($"Production Rate changed to: {newProductionRate}%");
    }

    private static async Task UpdateReportedProductionRateAsync(int currentProductionRate)
    {
        var reportedProperties = new TwinCollection();
        reportedProperties["ProductionRate"] = currentProductionRate;

        await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
        Console.WriteLine($"Reported Production Rate updated to: {currentProductionRate}%");
    }

    private static async Task SendMessageToIoTHub(object telemetryData)
    {
        var messageString = JsonConvert.SerializeObject(telemetryData);
        var message = new Message(Encoding.ASCII.GetBytes(messageString));

        await deviceClient.SendEventAsync(message);
        Console.WriteLine("Telemetry message sent to IoT Hub");
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

    private static async Task<int> GetReportedDeviceErrorAsync()
    {
        Twin twin = await deviceClient.GetTwinAsync();
        if (twin.Properties.Reported.Contains("DeviceError"))
        {
            return (int)twin.Properties.Reported["DeviceError"];
        }
        return 0;
    }

    private static async Task SendDeviceErrorEventToIoTHub(object deviceError)
    {
        var errorMessageString = JsonConvert.SerializeObject(deviceError);
        var message = new Message(Encoding.ASCII.GetBytes(errorMessageString));
        message.Properties.Add("MessageType", "DeviceError");
        message.Properties.Add("EventType", "Error");

        await deviceClient.SendEventAsync(message);
        Console.WriteLine("DeviceError event sent to IoT Hub");
    }

    private static async Task UpdateReportedDeviceErrorAsync(int errorCode, string errorMessage)
    {
        var reportedProperties = new TwinCollection();
        reportedProperties["DeviceError"] = errorCode;
        await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
        Console.WriteLine($"Updated Reported Device Error: {errorCode}, {errorMessage}");
    }

    public static async Task CallEmergencyStop()
    {
        string deviceNode = "ns=2;s=Device 1";
        opcClient.CallMethod(deviceNode, $"{deviceNode}/EmergencyStop");
        Console.WriteLine("Emergency stop completed.");
    }

    public static async Task CallResetErrorStatus()
    {
        string deviceNode = "ns=2;s=Device 1";
        opcClient.CallMethod(deviceNode, $"{deviceNode}/ResetErrorStatus");
        await Task.Delay(1000);
        Console.WriteLine("Error status reset executed.");
    }
}

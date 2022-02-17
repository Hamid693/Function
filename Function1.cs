// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;

using Azure.DigitalTwins.Core;
using Azure.Identity;
using System.Net.Http;
using Azure.Core.Pipeline;

using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Azure;
using System.Text;

namespace windmilltwiningestfunction
{
    public static class Function1
    {
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient singletonHttpClientInstance = new HttpClient();
        private static TimeZoneInfo India_Standard_Time = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");

        [FunctionName("IOTHubtoTwins")]

#pragma warning disable AZF0001 // Avoid async void
        public async static void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
#pragma warning restore AZF0001 // Avoid async void
        {
            if (adtInstanceUrl == null) log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
            try
            {
                log.LogInformation(eventGridEvent.Data.ToString());
                var cred = new ManagedIdentityCredential("https://digitaltwins.azure.net");

                var client = new DigitalTwinsClient(
                       new Uri(adtInstanceUrl), cred,
                       new DigitalTwinsClientOptions
                       {
                           Transport = new HttpClientTransport(singletonHttpClientInstance)
                       });
                log.LogInformation($"ADT service client connection created.");
                AsyncPageable<BasicDigitalTwin> asyncPageableResponse = client.QueryAsync<BasicDigitalTwin>("SELECT * FROM digitaltwins");

                // Iterate over the twin instances in the pageable response.
                // The "await" keyword here is required because new pages will be fetched when necessary,
                // which involves a request to the service.
                await foreach (BasicDigitalTwin twin in asyncPageableResponse)
                {
                    log.LogInformation($"Found digital twin '{twin.Id}' and Contents: '{twin.Contents}'");
                }

                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    log.LogInformation(eventGridEvent.Data.ToString());

                    // convert the message into a json object
                    JObject deviceMessage = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                    byte[] data = Convert.FromBase64String(deviceMessage["body"].ToString());
                    string bodyMsgStr = Encoding.UTF8.GetString(data);

                    //string bodyMsgStr = Convert.FromBase64String(Encoding.UTF8.GetString((byte[])deviceMessage["body"]));
                    //log.LogInformation("Body Message is:{deviceMessage}", deviceMessage["body"].ToString());
                    log.LogInformation("Body Message bodyMsgStr is:{bodyMsgStr}", bodyMsgStr);

                    JObject bodyMsgJson = (JObject)JsonConvert.DeserializeObject(bodyMsgStr);

                    string IotdeviceId = (string)deviceMessage["systemProperties"]["iothub-connection-device-id"];
                    log.LogInformation("Device ID is: {deviceId}", IotdeviceId);
                    var updateTwinData = new JsonPatchDocument();
                    string SubsystemID = (string)bodyMsgJson["subsystemID"];
                    //string systemID = (string)bodyMsgJson["systemID"];
                   if (SubsystemID == "farm")
                    {
                        string subsystemID = (string)bodyMsgJson["subsystemID"];
                        int messageId = (int)bodyMsgJson["messageId"];
                        DateTime dateTime_Indian = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, India_Standard_Time);
                       // DateTime Datetime = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Local);
                        string s = String.Format("{0:s}", dateTime_Indian);
                        //DateTime Datetime = (DateTime)bodyMsgJson["Datetime"];
                        //double Latitude = (double)bodyMsgJson["Latitude"];
                        //double Longitude = (double)bodyMsgJson["Longitude"];
                        log.LogInformation($"Device ID:{IotdeviceId} subsystemID is:{subsystemID} ");
                        //var updateTwinData = new JsonPatchDocument();
                        updateTwinData.AppendReplace("/subsystemID", subsystemID);
                        //updateTwinData.AppendReplace("/Longitude", Longitude);
                        updateTwinData.AppendReplace("/Datetime",s);
                       // updateTwinData.AppendReplace("/Latitude", Latitude);
                        //await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);

                    }
                    else if (SubsystemID == "Turbine")

                    {

                        string subsystemID = (string)bodyMsgJson["subsystemID"];
                        int messageId = (int)bodyMsgJson["messageId"];
                        double Latitude = (double)bodyMsgJson["Latitude"];
                        double Longitude = (double)bodyMsgJson["Longitude"];
                        string Connectivity = (string)bodyMsgJson["Connectivity"];
                        int SpeedOfWindblade = (int)bodyMsgJson["SpeedOfWindblade"];
                        double Accelerometer_x = (double)bodyMsgJson["Accelerometer_x"];
                        double Accelerometer_y = (double)bodyMsgJson["Accelerometer_y"];
                        double Accelerometer_z = (double)bodyMsgJson["Accelerometer_z"];
                        log.LogInformation($"Device ID:{IotdeviceId} Connectivity is :{Connectivity} subsystemID is:{subsystemID} Latitude is:{Latitude} And Longitude is:{Longitude} SpeedOfWindblade is:{SpeedOfWindblade} Accelerometer_x : {Accelerometer_x} Accelerometer_y : {Accelerometer_y} Accelerometer_z : {Accelerometer_z}");

                        //var updateTwinData = new JsonPatchDocument();

                        updateTwinData.AppendReplace("/Connectivity", Connectivity);
                        updateTwinData.AppendReplace("/subsystemID", subsystemID);
                        updateTwinData.AppendReplace("/Longitude", Longitude);
                        updateTwinData.AppendReplace("/Latitude", Latitude);
                        updateTwinData.AppendReplace("/SpeedOfWindblade", SpeedOfWindblade);
                        updateTwinData.AppendReplace("/Accelerometer_x", Accelerometer_x);
                        updateTwinData.AppendReplace("/Accelerometer_y", Accelerometer_y);
                        updateTwinData.AppendReplace("/Accelerometer_z", Accelerometer_z);

                        //await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);
                    }
                    else if (SubsystemID == "Nacelle")
                    {
                        string subsystemID = (string)bodyMsgJson["subsystemID"];
                        string Connectivity = (string)bodyMsgJson["Connectivity"];
                        int messageId = (int)bodyMsgJson["messageId"];
                        double temperature = (double)bodyMsgJson["temperature"]; //Math.Round((double)bodyMsgJson["temperature"], 4);
                        double humidity = (double)bodyMsgJson["humidity"]; //Math.Round((double)bodyMsgJson["humidity"], 4);
                        log.LogInformation($"Device ID:{IotdeviceId} subsystemID is:{subsystemID} Temperature is:{temperature} and Humidity is:{humidity}");
                        //var updateTwinData = new JsonPatchDocument();
                        updateTwinData.AppendReplace("/subsystemID", subsystemID);
                        updateTwinData.AppendReplace("/Connectivity", Connectivity);
                        updateTwinData.AppendReplace("/humidity", humidity);
                        updateTwinData.AppendReplace("/temperature", temperature);

                        // await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);


                    }
                    else if (SubsystemID == "Generator")
                    {
                        string subsystemID = (string)bodyMsgJson["subsystemID"];
                        string Connectivity = (string)bodyMsgJson["Connectivity"];
                        int messageId = (int)bodyMsgJson["messageId"];
                        double temperature = (double)bodyMsgJson["temperature"]; //Math.Round((double)bodyMsgJson["temperature"], 4);
                        double vibration = (double)bodyMsgJson["vibration"]; //Math.Round((double)bodyMsgJson["humidity"], 4);
                        log.LogInformation($"Device ID:{IotdeviceId} subsystemID is:{subsystemID} Temperature is:{temperature} and Vibration is:{vibration}");
                        //var updateTwinData = new JsonPatchDocument();
                        updateTwinData.AppendReplace("/subsystemID", subsystemID);
                        updateTwinData.AppendReplace("/Connectivity", Connectivity);
                        updateTwinData.AppendReplace("/temperature", temperature);
                        updateTwinData.AppendReplace("/vibration", vibration);
                        //await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);

                    }
                    
                    else if (SubsystemID == "GearBox")
                    {
                        string subsystemID = (string)bodyMsgJson["subsystemID"];
                        string Connectivity = (string)bodyMsgJson["Connectivity"];
                        int messageId = (int)bodyMsgJson["messageId"];
                        double temperature = (double)bodyMsgJson["temperature"]; //Math.Round((double)bodyMsgJson["temperature"], 4);
                        log.LogInformation($"Device ID:{IotdeviceId} subsystemID is:{subsystemID} and Temperature is:{temperature}");
                        //var updateTwinData = new JsonPatchDocument();
                        updateTwinData.AppendReplace("/subsystemID", subsystemID);
                        updateTwinData.AppendReplace("/Connectivity", Connectivity);
                        updateTwinData.AppendReplace("/temperature", temperature);
                        //await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);

                    }
                   
                    else
                    {
                        log.LogInformation($"Warning!!! Device ID not matched with existing list. Device ID : {IotdeviceId}");
                    }
                    await client.UpdateDigitalTwinAsync(IotdeviceId, updateTwinData);

                }
            }
            catch (Exception ex)
            {
                log.LogError($"Error in ingest function: {ex.Message}");
            }
        }
    }
}

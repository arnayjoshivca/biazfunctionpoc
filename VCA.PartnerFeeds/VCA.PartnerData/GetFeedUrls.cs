using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace VCA.PartnerData
{
    public static class GetFeedUrls
    {
        [FunctionName("GetFeedUrls")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("GetFeedUrls - HTTP trigger function processed a request.");
            string returnJson = string.Empty;
            try
            {
                //variable initialization
                int allowedDataAccessInDays = 30;
                int sasActiveTimeInHrs = 8;

                //authenticate and get partner name
                //get the parameters and check for validity
                dynamic data = await req.Content.ReadAsAsync<object>();
                var param = ParseRequestParameters(req, data, AuthorizeAndGetPartnerName(req), allowedDataAccessInDays);

                //return data
                List<string> sasUrls = GetFeedSecureAccessUrls(param, sasActiveTimeInHrs);
                var returnObject = new { feedName = param.Item2, sasUrls };
                returnJson = JsonConvert.SerializeObject(returnObject);
            }
            catch (ArgumentException ae)
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, ae.Message);
            }
            catch (Exception ex)
            {
                //log error and show custom message to customers
                log.Error(ex.ToString());
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "Internal error occured. Contact VCA Data team for help.");
            }
            return req.CreateResponse(HttpStatusCode.OK, returnJson, "application/json");
        }

        private static (string partnerName, string feedName, DateTime startDate, DateTime endDate) ParseRequestParameters(HttpRequestMessage req, dynamic data, string partnerName, int allowedDataAccessInDays = 1)
        {
            //get values from query parameters
            string feedName = req.GetQueryNameValuePairs().FirstOrDefault(q => string.Compare(q.Key, "feed", true) == 0).Value;
            string startdt = req.GetQueryNameValuePairs().FirstOrDefault(q => string.Compare(q.Key, "startdate", true) == 0).Value;
            string enddt = req.GetQueryNameValuePairs().FirstOrDefault(q => string.Compare(q.Key, "enddate", true) == 0).Value;

            //get values from the post request
            feedName = feedName ?? data?.feed;
            startdt = startdt ?? data?.startdate;
            enddt = enddt ?? data?.enddate;

            //get the dates and check for errors
            DateTime startDate, endDate;
            bool isValidStartDate = DateTime.TryParse(startdt, out startDate);
            bool isValidEndDate = DateTime.TryParse(enddt, out endDate);

            if (string.IsNullOrEmpty(feedName) || !isValidStartDate || !isValidEndDate)
                throw new ArgumentException("Invalid parameters: Invalid or Null or empty values passed for either of the paramters 'feed, startdate, enddate'");

            if (startDate.Date > endDate.Date)
                throw new ArgumentException("Invalid parameters: enddate is smaller than startdate");

            if (startDate.Date > DateTime.Now.Date || endDate.Date > DateTime.Now.Date)
                throw new ArgumentException("Invalid parameters: startdate and enddate should be smaller than today's date");

            if ((DateTime.Now.Date - startDate.Date).TotalDays > allowedDataAccessInDays)
                throw new ArgumentException($"Invalid parameters: Date range exceeds the allowed history data pull for last {allowedDataAccessInDays} days");

            //check for feed name
            List<string> feeds = GetExposedFeedNamesForPartner(partnerName);
            if (null == feeds)
                throw new ArgumentException($"Invalid parameters: Feeds are not exposed for the partner '{partnerName}'.");

            if (!feeds.Contains(feedName.ToLower()))
                throw new ArgumentException($"Invalid parameters: Requested feed '{feedName}' is not exposed for the partner '{partnerName}'.");

            return (partnerName, feedName, startDate, endDate);
        }

        private static List<string> GetFeedSecureAccessUrls(ValueTuple<string, string, DateTime, DateTime> param, int sasActiveTimeInHrs = 1)
        {
            List<string> sasUrls = new List<string>();
            var (partnerName, feedName, startDate, endDate) = param;

            string blobUrl = "https://mabiodsstoredev.blob.core.windows.net";
            string containerName = "partners";
            string partnerFolder = string.Concat(partnerName, "/", feedName);
            var blobConStr = "DefaultEndpointsProtocol=https;AccountName=mabiodsstoredev;AccountKey=NJ4VDlkZswD8YaYqwqFPuxBK8qNqS6yeBHpBztCuJnnvHYU3yua2MgoEAwPAyfRFolE4CUET8UHA7ipsJglLzg==;EndpointSuffix=core.windows.net";
            var blobClient = CloudStorageAccount.Parse(blobConStr).CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            for (DateTime date = startDate.Date; date.Date <= endDate.Date; date = date.AddDays(1))
            {
                string fileName = $"{partnerFolder}/{date.Year}/{date.Month.ToString("d2")}/{date.Day}/{feedName}.csv";
                var blob = container.GetBlobReference(fileName);
                var task = blob.ExistsAsync();
                task.Wait();
                bool blobExists = task.Result;
                string sas = string.Empty;
                if (blobExists)
                {
                    var policy = GetSasPolicy(sasActiveTimeInHrs);
                    sas = $@"{blobUrl}/{containerName}/{fileName}{blob.GetSharedAccessSignature(policy)}";
                }
                else
                    sas = $"Blob not found for the date: {date}";
                sasUrls.Add(sas);
            }
            return sasUrls;
        }

        private static List<string> GetExposedFeedNamesForPartner(string partnerName)
        {
            NameValueCollection exposedFeed = new NameValueCollection();
            exposedFeed.Add("TargetBase", "Client");
            exposedFeed.Add("TargetBase", "Patient");

            if (null == exposedFeed[partnerName])
                return null;
            else
                return exposedFeed[partnerName].ToLower().Split(',').ToList<string>();
        }

        private static string AuthorizeAndGetPartnerName(HttpRequestMessage req)
        {
            string partnerName = string.Empty;
            string loginName = string.Empty;
            var identity = Thread.CurrentPrincipal.Identity;
            if (identity.IsAuthenticated)
            {
                //get the email name from the auth
                /*
                var task = req.GetAuthInfoAsync();
                task.Wait();
                AuthInfo authInfo = task.Result;
                var emailClaim = authInfo?.GetClaim(ClaimTypes.Email);
                */
                if (identity.AuthenticationType.ToLower() != "aad")
                {
                    throw new ArgumentException("Unsupported authentication method. Contact VCA Data team for access details.");
                }
                partnerName = GetPartnerName(identity.Name);
                if (string.IsNullOrEmpty(partnerName))
                { 
                    throw new ArgumentException("Un-Authorized request. Contact VCA Data team for access.");
                }
            }
            else
            {
                throw new ArgumentException("Un-Authorized request. Contact VCA Data team for access.");
            }
            
            return partnerName;
        }

        private static SharedAccessBlobPolicy GetSasPolicy(int sasActiveTimeInHrs)
        {
            return new SharedAccessBlobPolicy
            {
                SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5),
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(sasActiveTimeInHrs),
                Permissions = SharedAccessBlobPermissions.Read
            };
        }

        private static string GetPartnerName(string userLogin)
        {
            //to-do we need to call this based on azure table
            NameValueCollection partnerNames = new NameValueCollection();
            partnerNames.Add("arnay.joshi@vca.com", "TargetBase");
            partnerNames.Add("raj.tuteja@vca.com", "TargetBase");
            partnerNames.Add("kevin.loo@vca.com", "TargetBase");
            partnerNames.Add("Pinal.Dave@vca.com", "TargetBase");
            
            if (null == partnerNames[userLogin])
                return null;
            else
                return partnerNames[userLogin].Split(',').FirstOrDefault<string>();
        }

    }
}

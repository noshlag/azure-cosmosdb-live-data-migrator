namespace MigrationConsoleApp
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using System.Xml.XPath;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using ChangeFeedObserverCloseReason = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.ChangeFeedObserverCloseReason;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;
    using MsnUser = Msn.TagsDataModel.TagEntityLib.User;

    public class DocumentFeedObserver: IChangeFeedObserver
    {
        private readonly DocumentClient client;
        private readonly Uri destinationCollectionUri;
        private IBulkExecutor bulkExecutor;
        private IDocumentTransformer documentTransformer;
        private BlobContainerClient containerClient;
        private readonly string SourcePartitionKeys;
        private readonly string TargetPartitionKey;

        public DocumentFeedObserver(string SourcePartitionKeys, string TargetPartitionKey, DocumentClient client, DocumentCollectionInfo destCollInfo, IDocumentTransformer documentTransformer, BlobContainerClient containerClient)
        {
            this.SourcePartitionKeys = SourcePartitionKeys;
            this.TargetPartitionKey = TargetPartitionKey;
            this.client = client;
            this.destinationCollectionUri = UriFactory.CreateDocumentCollectionUri(destCollInfo.DatabaseName, destCollInfo.CollectionName);
            this.documentTransformer = documentTransformer;
            this.containerClient = containerClient;  
        }

        public async Task OpenAsync(IChangeFeedObserverContext context)
        {
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 100000;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 100000;

            DocumentCollection destinationCollection = await client.ReadDocumentCollectionAsync(this.destinationCollectionUri);
            bulkExecutor = new BulkExecutor(client, destinationCollection);
            client.ConnectionPolicy.UserAgentSuffix = (" migrationService");

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine("Observer opened for partition Key Range: {0}", context.PartitionKeyRangeId);
            Console.ForegroundColor = ConsoleColor.White;

            await bulkExecutor.InitializeAsync();
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Observer closed, {0}", context.PartitionKeyRangeId);
            Console.WriteLine("Reason for shutdown, {0}", reason);
            Console.ForegroundColor = ConsoleColor.White;

            return Task.CompletedTask;
        }

        public static JsonSerializerSettings jsonSettings = new JsonSerializerSettings()
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            NullValueHandling = NullValueHandling.Ignore
        };

        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context, 
            IReadOnlyList<Document> docs, 
            CancellationToken cancellationToken)
        {
            BulkImportResponse bulkImportResponse = new BulkImportResponse();
            try
            {
                Boolean isSyntheticKey = SourcePartitionKeys.Contains(",");
                Boolean isNestedAttribute = SourcePartitionKeys.Contains("/");

                List<Document> transformedDocs = new List<Document>();
                Document document = new Document();
                var newDocs = docs.Where(doc =>
                {
                    string typeName = doc.GetPropertyValue<string>("_t");
                    if (typeName == null) return false;  // If we have no typeName, then it definitely isn't a User or Action
                    if (typeName.Equals("Action")) return true;  // All actions should be copied over
                    if (typeName.Equals("User"))  //Only non-empty User actions should be copied over. 
                    {
                        try
                        {
                            //I'm not certain about this method of converting the Document into an MsnUser.  I might need to change this line
                            MsnUser msnUser = JsonConvert.DeserializeObject<MsnUser>(JsonConvert.SerializeObject(doc, jsonSettings));


                            if (!string.IsNullOrWhiteSpace(msnUser.Email))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.UserName))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.UserProfile))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.UserPictureUrl))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.UserLevel))
                                return true; //Never going to happen

                            if (msnUser.IsMigrationConsentGiven != null)
                                return true;
                            if (msnUser.NewsAppMigrated?.Count > 0)
                                return true;
                            if (!string.IsNullOrEmpty(msnUser.UserLayoutPreference))
                                return true;
                            if (!string.IsNullOrWhiteSpace(msnUser.WeatherDisplayUnit))
                                return true;
                            if (msnUser.HasActions?.Count > 0)
                                return true;
                            if (msnUser.AnyActions != null)
                                return true;
                            if (msnUser.PdpFacetsMigrated?.Count > 0)
                                return true;

                            /* Vishakha Said to remove because she owns them and knows they are not needed
                            if (msnUser.FinancePdpMigrated?.Count > 0)              return true; //Never going to happen
                            if (msnUser.SportsPdpMigrated?.Count > 0)               return true; //Never going to happen
                            if (msnUser.LocationPdpMigrated?.Count > 0)             return true; //Never going to happen
                            if (msnUser.NewsPdpMigrated)                            return true; //Never going to happen
                            */

                            if (!string.IsNullOrWhiteSpace(msnUser.EntityName))
                                return true; //Never going to happen
                            if (msnUser.PageViewsPropertyBag?.Count > 0)
                                return true; //Never going to happen
                            if (msnUser.PdpMarketMigrated == true)
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.PrimeMarketPreference))
                                return true; //Never going to happen

                            if (msnUser.PrimeMarketConfig != null)
                                return true;
                            if (msnUser.PrimeStripeSelection != null)
                                return true;

                            if (msnUser.PdpStripeMigrated == true)
                                return true; //Never going to happen
                            if (msnUser.AnaheimSettings != null)
                                return true; //Never going to happen

                            if (msnUser.PdpWatchlistSetting != null)
                                return true;

                            if (!string.IsNullOrWhiteSpace(msnUser.Type))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.Url))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.SourceHref))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.Locale))
                                return true; //Never going to happen
                            if (!string.IsNullOrWhiteSpace(msnUser.AdaptiveCard))
                                return true; //Never going to happen

                            if (!string.IsNullOrWhiteSpace(msnUser.SegmentCardSettings))
                                return true;
                            if (msnUser.UserSettings?.Count > 0)
                                return true;

#if false // This was the old attempt at the code.  I'm keeping it around for now because it has some of my notes about which properties are ignorable.
                            // Run a bunch of tests on the resulting msnAction document.
                            //if (doc.GetPropertyValue<string>("email") != null)                      return true; //None
                            //if (doc.GetPropertyValue<string>("userName") != null)                   return true; //None
                            //if (doc.GetPropertyValue<string>("userProfile") != null)                return true; //None
                            //if (doc.GetPropertyValue<string>("userPictureUrl") != null)             return true; //None
                            //if (doc.GetPropertyValue<string>("userLevel") != null)                  return true; //None
                            if (doc.GetPropertyValue<string>("isMigrationConsentGiven") != null)    return true; //Has Results
                            if (doc.GetPropertyValue<object>("newsAppMigrated") != new object())    return true; //Has Results
                            if (doc.GetPropertyValue<string>("userLayoutPreference") != null)       return true; //Has Results
                            if (doc.GetPropertyValue<string>("weatherDisplayUnit") != null)         return true; //Has Results
                            if (doc.GetPropertyValue<object>("hasActions") != null
                                && doc.GetPropertyValue<object>("hasActions") != new object())      return true; //Has Results 
                            if (doc.GetPropertyValue<bool?>("anyActions") != null)                  return true; //Has Results
                            if (doc.GetPropertyValue<object>("pdpFacetsMigrated") != new object())  return true; //Has Results
                            //if (doc.GetPropertyValue<object>("financePdpMigrated") != new object())     return true;
                            //if (doc.GetPropertyValue<object>("sportsPdpMigrated") != new object())      return true;
                            //if (doc.GetPropertyValue<object>("locationPdpMigrated") != new object())    return true;
                            //if (doc.GetPropertyValue<bool>("newsPdpMigrated") != false)                 return true;
                            //if (doc.GetPropertyValue<string>("name") != null)                       return true;
                            //if (doc.GetPropertyValue<object>("pageViewsPropertyBag") != new object())   return true;
                            //if (doc.GetPropertyValue<bool>("pdpMarketMigrated") != false)               return true; //Only False
                            //if (doc.GetPropertyValue<string>("primeMarketPreference") != null)      return true;//None
                            if (doc.GetPropertyValue<object>("primeMarketConfig") != null)          return true; //Has Results
                            if (doc.GetPropertyValue<object>("primeStripeSelection") != null) return true; //Has Results
                            //if (doc.GetPropertyValue<bool>("pdpStripeMigrated") != false)               return true; //No Results
                            //if (doc.GetPropertyValue<string>("anaheimSettings") != null)            return true; //No Results
                            if (doc.GetPropertyValue<object>("pdpWatchlistSetting") != null)        return true; //Has Results
                            //if (doc.GetPropertyValue<string>("type") != null)                       return true;// One Useless Result
                            //if (doc.GetPropertyValue<string>("url") != null)                        return true;//No Results
                            //if (doc.GetPropertyValue<string>("sourceHref") != null)                 return true;//No Results
                            //if (doc.GetPropertyValue<string>("locale") != null)                     return true;//No Results
                            //if (doc.GetPropertyValue<string>("adaptiveCard") != null)               return true;//No Results
#endif

                        }
                        catch (Exception ex)
                        {
                            //We may want to add some logging here.
                            return false;
                        }
                    }
                    return false;
                });

                if (newDocs.Any())
                {
                    // Apply any necessary Partition Key mappings and transform the documents.  For the User and Action migration, this will be a NOOP, 
                    // but it's still worth keeping the code in so that we have access to the functionality if it turns out we need it later.
                    foreach (var doc in newDocs)
                    {
                        document = (SourcePartitionKeys != null & TargetPartitionKey != null) ? MapPartitionKey(doc, isSyntheticKey, TargetPartitionKey, isNestedAttribute, SourcePartitionKeys) : document = doc;
                        transformedDocs.AddRange(documentTransformer.TransformDocument(document).Result);
                    }

                    bulkImportResponse = await bulkExecutor.BulkImportAsync(
                        documents: transformedDocs.ToList(),
                        enableUpsert: false,
                        maxConcurrencyPerPartitionKeyRange: 1,
                        disableAutomaticIdGeneration: true,
                        maxInMemorySortingBatchSize: null,
                        cancellationToken: new CancellationToken());

                    if (bulkImportResponse.FailedImports.Count > 0 && containerClient != null)
                    {
                        WriteFailedDocsToBlob("FailedImportDocs", containerClient, bulkImportResponse);
                    }

                    if (bulkImportResponse.BadInputDocuments.Count > 0 && containerClient != null)
                    {
                        WriteFailedDocsToBlob("BadInputDocs", containerClient, bulkImportResponse);
                    }


                    LogMetrics(context, bulkImportResponse);
                }
            }
            catch (Exception e)
            {
                Program.telemetryClient.TrackException(e);
            }

            Program.telemetryClient.Flush();
        }

        private static void WriteFailedDocsToBlob(string failureType, BlobContainerClient containerClient, BulkImportResponse bulkImportResponse)
        {
            string failedDocs;
            byte[] byteArray;
            BlobClient blobClient = containerClient.GetBlobClient(failureType + Guid.NewGuid().ToString() + ".csv");
            if (failureType == "FailedImportDocs")
            {
                failedDocs = JsonConvert.SerializeObject(String.Join(",", bulkImportResponse.FailedImports.First().DocumentsFailedToImport));
                byteArray = Encoding.ASCII.GetBytes(bulkImportResponse.FailedImports.First().BulkImportFailureException.GetType() + "|" + bulkImportResponse.FailedImports.First().DocumentsFailedToImport.Count + "|" + bulkImportResponse.FailedImports.First().BulkImportFailureException.Message.Substring(0, 100) + "|" + failedDocs);
            }
            else
            {
                failedDocs = JsonConvert.SerializeObject(String.Join(",", bulkImportResponse.BadInputDocuments));
                byteArray = Encoding.ASCII.GetBytes(failureType + ", " + bulkImportResponse.BadInputDocuments.Count + "|" + failedDocs);
            }
            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }

        private static void LogMetrics(IChangeFeedObserverContext context, BulkImportResponse bulkImportResponse)
        {
            Program.telemetryClient.TrackMetric("TotalInserted", bulkImportResponse.NumberOfDocumentsImported);
            Program.telemetryClient.TrackMetric("InsertedDocuments-Process:"
                + Process.GetCurrentProcess().Id, bulkImportResponse.NumberOfDocumentsImported);
            Program.telemetryClient.TrackMetric("TotalRUs", bulkImportResponse.TotalRequestUnitsConsumed);

            if (bulkImportResponse.BadInputDocuments.Count > 0)
            {
                Program.telemetryClient.TrackMetric("BadInputDocsCount", bulkImportResponse.BadInputDocuments.Count);
            }

            if (bulkImportResponse.FailedImports.Count > 0)
            {
                Program.telemetryClient.TrackMetric("FailedImportDocsCount", bulkImportResponse.FailedImports.First().DocumentsFailedToImport.Count);
            }
        }

        public static Document MapPartitionKey(Document doc, Boolean isSyntheticKey, string targetPartitionKey, Boolean isNestedAttribute, string sourcePartitionKeys)
        {            
            if (isSyntheticKey)
            {
                doc = CreateSyntheticKey(doc, sourcePartitionKeys, isNestedAttribute, targetPartitionKey);
            }
            else
            {
                doc.SetPropertyValue(targetPartitionKey, isNestedAttribute == true ? GetNestedValue(doc, sourcePartitionKeys): doc.GetPropertyValue<string>(sourcePartitionKeys));      
            }
            return doc;
        }

        public static Document CreateSyntheticKey(Document doc, string sourcePartitionKeys, Boolean isNestedAttribute, string targetPartitionKey)
        {
            StringBuilder syntheticKey = new StringBuilder();
            string[] sourceAttributeArray = sourcePartitionKeys.Split(',');
            int arraylength = sourceAttributeArray.Length;
            int count = 1;
            foreach (string rawattribute in sourceAttributeArray)
            {
                string attribute = rawattribute.Trim();
                if (count == arraylength)
                {
                    string val = isNestedAttribute == true ? GetNestedValue(doc, attribute) : doc.GetPropertyValue<string>(attribute);
                    syntheticKey.Append(val);
                }
                else
                {
                    string val = isNestedAttribute == true ? GetNestedValue(doc, attribute) + "-" : doc.GetPropertyValue<string>(attribute) + "-";
                    syntheticKey.Append(val);
                }
                count++;
            }
            doc.SetPropertyValue(targetPartitionKey, syntheticKey.ToString());
            return doc;
        }

        public static string GetNestedValue (Document doc, string path)
        {
            var jsonReader = JsonReaderWriterFactory.CreateJsonReader(Encoding.UTF8.GetBytes(doc.ToString()), new System.Xml.XmlDictionaryReaderQuotas());
            var root = XElement.Load(jsonReader);
            string value = root.XPathSelectElement("//"+path).Value;
            return value;
        }
    }
}

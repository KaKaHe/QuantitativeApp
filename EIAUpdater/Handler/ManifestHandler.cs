using EIAUpdater.Database;
using EIAUpdater.Model;
using log4net;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace EIAUpdater.Handler
{
    public class ManifestHandler
    {
        private static ILog logger = LogManager.GetLogger(typeof(ManifestHandler));
        private string LocalFileName { get; set; }
        public Configurations Configurations;

        public ManifestHandler(Configurations config)
        {
            Configurations = config;
            LocalFileName = DateStampFile(GetFileName(config.Manifest), "_yyyyMMdd");
        }

        public bool Download()
        {
            logger.Info("Start downloading today's manifest");
            if (File.Exists(Path.Combine(Configurations.LocalFolder, LocalFileName)))
            {
                File.Move(Path.Combine(Configurations.LocalFolder, LocalFileName), Path.Combine(Configurations.LocalFolder, DateStampFile(LocalFileName, "HHmmss")));
            }
            FileHandler handler = new FileHandler(Configurations.Manifest);
            Task<string> task = handler.Download(Configurations.LocalFolder, LocalFileName);
            Task.WaitAll();
            string manifest = task.Result;

            if (!string.IsNullOrEmpty(manifest) && !manifest.Equals("Failed"))
                return true;

            return false;
        }

        public List<DataSetSummary> Parsing(MongoAgent conn)
        {
            logger.Info("Start parsing today's manifest");
            List<DataSetSummary> summary = new List<DataSetSummary>();

            StreamReader sr = new StreamReader(Path.Combine(Configurations.LocalFolder, LocalFileName));
            JObject jsonStr = JObject.Parse(sr.ReadToEnd());

            foreach (JToken token in jsonStr.SelectToken("dataset").Children())
            {
                //Deserialize the token from JSON to object
                DataSetSummary dataSetSum = JsonConvert.DeserializeObject<DataSetSummary>(token.First.ToString());
                dataSetSum.token = token.Path;

                //Compare with the last record to decide if it needs to be download today.
                BsonDocument query = new BsonDocument("identifier", dataSetSum.identifier);
                List<BsonDocument> list = conn.ReadCollection(Configurations.ManifestCollection, query);
                if (list.Count == 0)
                {
                    //If there is no record of such identifier, it means this is a new file type. It needs to be downloaded and insert into database.
                    summary.Add(dataSetSum);
                    conn.InsertCollectionAsync(Configurations.ManifestCollection, BsonDocument.Parse(token.First.ToString()));
                }
                else
                {
                    //Always only check the top 1 record to decide if a downloading need to be performed or not.
                    DataSetSummary old = BsonSerializer.Deserialize<DataSetSummary>(list[0]);

                    if (DateTime.Parse(dataSetSum.last_updated) > DateTime.Parse(old.last_updated))
                    {
                        summary.Add(dataSetSum);
                        //update performing.
                        query = new BsonDocument("_id", old._id);
                        BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(dataSetSum));
                        doc.SetElement(new BsonElement("_id", old._id));
                        conn.UpdateCollectionAsync(Configurations.ManifestCollection, query, doc);
                    }
                }
            }

            logger.Info("Finish parsing today's manifest.");
            logger.Info("There are " + summary.Count + " datasets need to update.");
            return summary;
        }

        private string GetFileName(string FullName)
        {
            Uri fileUri = new Uri(FullName);
            return fileUri.Segments.GetValue(fileUri.Segments.Length - 1).ToString();
            //return string.Empty;
        }

        private string DateStampFile(string fileName, string stampFormat)
        {
            StringBuilder newName = new StringBuilder(fileName);
            if (fileName.Contains("."))
                newName.Insert(fileName.IndexOf("."), DateTime.Now.ToString(stampFormat));
            else
                newName.Append(DateTime.Now.ToString(stampFormat));
            return newName.ToString();
        }
    }
}

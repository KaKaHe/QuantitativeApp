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
    public class ManifestHandler : IHandler
    {
        //private static ILog logger = LogManager.GetLogger(typeof(ManifestHandler));
        public ILog Logger { get; set; }
        private string LocalFileName { get; set; }
        public Configurations Config { get; set; }

        public ManifestHandler(Configurations config)
        {
            Config = config;
            LocalFileName = DateStampFile(GetFileName(config.Manifest), "_yyyyMMdd");
            Logger = LogManager.GetLogger(typeof(ManifestHandler));
        }

        public bool Download()
        {
            Logger.Info("Start downloading today's manifest");
            if (File.Exists(Path.Combine(Config.LocalFolder, LocalFileName)))
            {
                File.Move(Path.Combine(Config.LocalFolder, LocalFileName), Path.Combine(Config.LocalFolder, DateStampFile(LocalFileName, "HHmmss")));
            }
            FileHandler handler = new FileHandler(Config.Manifest);
            Task<string> task = handler.Download(Config.LocalFolder, LocalFileName);
            Task.WaitAll();
            string manifest = task.Result;

            if (!string.IsNullOrEmpty(manifest) && !manifest.Equals("Failed"))
                return true;

            return false;
        }

        public List<DataSetSummary> ParsingData(MongoAgent conn)
        {
            Logger.Info("Start parsing today's manifest");
            List<DataSetSummary> summary = new List<DataSetSummary>();

            try
            {
                StreamReader sr = new StreamReader(Path.Combine(Config.LocalFolder, LocalFileName));
                JObject jsonStr = JObject.Parse(sr.ReadToEnd());

                foreach (JToken token in jsonStr.SelectToken("dataset").Children())
                {
                    //Deserialize the token from JSON to object
                    DataSetSummary dataSetSum = JsonConvert.DeserializeObject<DataSetSummary>(token.First.ToString());
                    dataSetSum.token = token.Path;

                    //Compare with the last record to decide if it needs to be download today.
                    BsonDocument query = new BsonDocument("identifier", dataSetSum.identifier);
                    List<BsonDocument> list = conn.ReadCollection(Config.ManifestCollection, query);
                    if (list.Count == 0)
                    {
                        //If there is no record of such identifier, it means this is a new file type. It needs to be downloaded and insert into database.
                        summary.Add(dataSetSum);
                        conn.InsertCollectionAsync(Config.ManifestCollection, BsonDocument.Parse(token.First.ToString()));
                    }
                    else
                    {
                        //Always only check the top 1 record to decide if a downloading need to be performed or not.
                        DataSetSummary old = BsonSerializer.Deserialize<DataSetSummary>(list[0]);

                        if (DateTime.Parse(dataSetSum.last_updated) > DateTime.Parse(old.last_updated))
                        {
                            dataSetSum._id = old._id;
                            summary.Add(dataSetSum);
                            //update performing.
                            //query = new BsonDocument("_id", old._id);
                            //BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(dataSetSum));
                            //doc.SetElement(new BsonElement("_id", old._id));
                            //conn.UpdateCollectionAsync(Configurations.ManifestCollection, query, doc);
                        }
                    }
                }

                Logger.Info("Finish parsing today's manifest.");
                Logger.Info("There are " + summary.Count + " datasets need to update.");
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return new List<DataSetSummary>();
            }
            return summary;
        }

        public void UpdateManifest(DataSetSummary dataSet)
        {
            MongoAgent conn = MongoAgent.GetInstance(Config);
            BsonDocument query = new BsonDocument("_id", dataSet._id);
            BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(dataSet));
            doc.SetElement(new BsonElement("_id", dataSet._id));
            conn.UpdateCollectionAsync(Config.ManifestCollection, query, doc);
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

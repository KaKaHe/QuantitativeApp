using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using EIAUpdater.Model;
using System.Text;
using EIAUpdater.Database;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace EIAUpdater.Handler
{
    public class ManifestHandler
    {
        public static ILog logger = LogManager.GetLogger(typeof(ManifestHandler));
        private string Manifest { get; set; }
        private string LocalFolder { get; set; }
        private string LocalFileName { get; set; }
        private string ManifestCollection { get; set; }

        public ManifestHandler(Configurations config)
        {
            Manifest = config.Manifest;
            LocalFolder = config.LocalFolder;
            LocalFileName = DateStampFile(GetFileName(Manifest), "_yyyyMMdd");
            ManifestCollection = config.ManifestCollection;
        }

        public bool Download()
        {
            logger.Info("Start downloading today's manifest");
            if (File.Exists(Path.Combine(LocalFolder, LocalFileName)))
            {
                File.Move(Path.Combine(LocalFolder, LocalFileName), Path.Combine(LocalFolder, DateStampFile(LocalFileName, "HHmmss")));
            }
            FileHandler handler = new FileHandler(Manifest);
            Task<string> task = handler.DownloadWebRequest(LocalFolder, LocalFileName);
            Task.WaitAll();
            string manifest = task.Result;

            if (!string.IsNullOrEmpty(manifest) && !manifest.Equals("Failed"))
                return true;

            return false;
        }

        public List<DataSet> Parsing(MongoAgent conn)
        {
            logger.Info("Start parsing today's manifest");
            List<DataSet> summary = new List<DataSet>();
            //MongoAgent conn = GetConn();

            StreamReader sr = new StreamReader(Path.Combine(LocalFolder, LocalFileName));
            JObject jsonStr = JObject.Parse(sr.ReadToEnd());

            //JObject obj = (JObject)JToken.ReadFrom(new JsonTextReader(new StringReader(jsonStr)));

            //Manifest manifest = JsonConvert.DeserializeObject<Manifest>(jStr);
            foreach (JToken token in jsonStr.SelectToken("dataset").Children())
            {
                //Deserialize the token from JSON to object
                DataSet fs = JsonConvert.DeserializeObject<DataSet>(token.First.ToString());
                fs.token = token.Path;

                //Compare with the last record to decide if it needs to be download today.
                BsonDocument query = new BsonDocument("identifier", fs.identifier);
                List<BsonDocument> list = conn.ReadCollection(ManifestCollection, query);
                if (list.Count == 0)
                {
                    //If there is no record of such identifier, it means this is a new file type. It needs to be downloaded and insert into database.
                    summary.Add(fs);
                    conn.InsertCollectionAsync(ManifestCollection, BsonDocument.Parse(token.First.ToString()));
                }
                else
                {
                    //Always only check the top 1 record to decide if a downloading need to be performed or not.
                    DataSet old = BsonSerializer.Deserialize<DataSet>(list[0]);

                    if (DateTime.Parse(fs.last_updated) > DateTime.Parse(old.last_updated))
                    {
                        summary.Add(fs);
                        //update performing.
                        query = new BsonDocument("_id", old._id);
                        BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(fs));
                        doc.SetElement(new BsonElement("_id", old._id));
                        //conn.replaceCollection(ManifestCollection, query, doc);
                        conn.UpdateCollectionAsync(ManifestCollection, query, doc);
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

using System;
using System.Net;
using System.IO;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Bson;
using System.Collections.Generic;
using EIAUpdater.Model;

namespace EIAUpdater
{
    class EIAUpdater
    {
        private string DataBaseName = "Quantitative";
        private string CollectionName = "EIA_Manifest";
        private string MongoDBAddress = "localhost";
        private int MongoDBPort = 27017;
        private string RemoteURL = "http://api.eia.gov/bulk/manifest.txt";
        private string LocalFolder = "C:\\Quantitative_Finance\\DataGrabing\\";
        private string LocalFileName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";

        static void Main(string[] args)
        {
            //Console.WriteLine("Hello World!");
            EIAUpdater eia = new EIAUpdater();
            //ObjectId id = new ObjectId(DateTime.UtcNow, Environment.MachineName.GetHashCode()/100, (short)System.Diagnostics.Process.GetCurrentProcess().Id, 1);
            //ObjectId id = cd.getObjectId(new ObjectId("5a808a103c99832c2c4fd3a7"));

            if (eia.Downloading())
            {
                //If the manifest file is downloaded successfully, continue doing parsing.
                List<FileSummary> dataList = eia.Parsing();

                //If there is no update, stop application.
            }
            else
            {
                //If the mainfest file downloaded failed, do something else other than parsing.
            }
            Console.ReadLine();
        }

        private bool Downloading()
        {
            //string strURL = "http://api.eia.gov/bulk/manifest.txt";
            //string strLocation = "C:\\Quantitative_Finance\\DataGrabing\\";
            //string strLocalName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";

            //Console.WriteLine("Current location: " + System.IO.Directory.GetCurrentDirectory().ToString());
            try
            {
                using (var client = new WebClient())
                {
                    if (!File.Exists(LocalFolder + LocalFileName))
                    {
                        client.DownloadFile(RemoteURL, LocalFolder + LocalFileName);
                    }
                }
                return true;
            }
            catch (WebException we)
            {
                Console.WriteLine(we.Message.ToString());
                return false;
            }
        }

        [Obsolete]
        private void logManifest()
        {
            MongoClientSettings clientSettings = new MongoClientSettings
            {
                Server = new MongoServerAddress("localhost", 27017),
                UseSsl = false
            };
            MongoAgent ma = MongoAgent.getInstance(clientSettings);
            ma.setDatabase("Quantitative");
            //ma.setCollection("EIA_Manifest");
            //var jsonStr = Parsing();
            List<FileSummary> list = Parsing();

            foreach (FileSummary fs in list)
            {
            }

            //var document = new BsonDocument(jsonStr);
            //ma.insertCollection("EIA_Manifest", Parsing());
            //ma.readCollection("EIA_Manifest");
        }

        private List<FileSummary> Parsing()
        {
            List<FileSummary> summary = new List<FileSummary>();
            MongoAgent conn = getConn();
            //string strManifest = "C:\\Quantitative_Finance\\DataGrabing\\manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";

            StreamReader sr = new StreamReader(LocalFolder + LocalFileName);
            JObject jsonStr = JObject.Parse(sr.ReadToEnd());

            //JObject obj = (JObject)JToken.ReadFrom(new JsonTextReader(new StringReader(jsonStr)));

            //Manifest manifest = JsonConvert.DeserializeObject<Manifest>(jStr);
            foreach (JToken token in jsonStr.SelectToken("dataset").Children())
            {
                //Deserialize the token from JSON to object
                FileSummary fs = JsonConvert.DeserializeObject<FileSummary>(token.First.ToString());
                fs.token = token.Path;

                //Compare with the last record to decide if it needs to be download today.
                BsonDocument query = new BsonDocument("identifier", fs.identifier);
                List<BsonDocument> list = conn.readCollection(CollectionName, query);
                if (list.Count == 0)
                {
                    //If there is no record of such identifier, it means this is a new file type. It needs to be downloaded and insert into database.
                    summary.Add(fs);
                    conn.InsertCollectionAsync(CollectionName, BsonDocument.Parse(token.First.ToString()));
                }
                else
                {
                    //Always only check the top 1 record to decide if a downloading need to be performed or not.
                    FileSummary old = BsonSerializer.Deserialize<FileSummary>(list[0]);

                    if (DateTime.Parse(fs.last_updated) > DateTime.Parse(old.last_updated))
                    {
                        summary.Add(fs);
                        //update performing.
                        query = new BsonDocument("_id", old._id);
                        BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(fs));
                        //doc.SetElement(new BsonElement("_id", getObjectId(old._id)));
                        doc.SetElement(new BsonElement("_id", old._id));
                        //conn.replaceCollection(CollectionName, query, doc);
                        conn.UpdateCollectionAsync(CollectionName, query, doc);
                    }
                }

                //summary.Add(fs);

                //conn.insertCollection("EIA_Manifest", token);
            }

            return summary;
        }

        private MongoAgent getConn()
        {
            MongoClientSettings clientSettings = new MongoClientSettings
            {
                Server = new MongoServerAddress(MongoDBAddress, MongoDBPort),
                UseSsl = false
            };
            MongoAgent ma = MongoAgent.getInstance(clientSettings);
            ma.setDatabase(DataBaseName);
            return ma;
        }

        [Obsolete]
        private ObjectId getObjectId(ObjectId oldone)
        {
            TimeSpan increment = DateTime.Now - oldone.CreationTime;
            return new ObjectId(oldone.CreationTime, Environment.MachineName.GetHashCode() / 100, (short)System.Diagnostics.Process.GetCurrentProcess().Id, increment.Milliseconds);
        }
    }
}

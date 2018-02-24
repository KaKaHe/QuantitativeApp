using EIAUpdater.Model;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;

namespace EIAUpdater
{
    class EIAUpdater
    {
        public string MongoDB = "";
        public string ManifestCollection = "";
        public string MongoHost = "";
        public int MongoDBPort = 3717;
        public string UserName = "";
        public string Password = "";
        public string Manifest = "http://api.eia.gov/bulk/manifest.txt";
        public string LocalFolder = "C:\\DataGrabing";
        public string LocalFileName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";
        //public event AsyncCompletedEventHandler DownloadCompleted;
        static void Main(string[] args)
        {
            //EIAUpdater eia = new EIAUpdater();
            EIAUpdater eia = Initializer();
            //ObjectId id = new ObjectId(DateTime.UtcNow, Environment.MachineName.GetHashCode()/100, (short)System.Diagnostics.Process.GetCurrentProcess().Id, 1);
            //ObjectId id = cd.getObjectId(new ObjectId("5a808a103c99832c2c4fd3a7"));

            if (eia.GetManifest())
            {
                //If the manifest file is downloaded successfully, continue doing parsing.
                List<FileSummary> dataList = eia.Parsing();
                List<Task> downloadlist = new List<Task>();
                List<Task> operationList = new List<Task>();
                int Count = 0;

                foreach (FileSummary fs in dataList)
                {
                    try
                    {
                        FileHandler handler = new FileHandler(fs.accessURL);
                        string downloadedFile = "";
                        string extractedFile = "";
                        Task download = Task.Factory.StartNew(() => downloadedFile = handler.Download(Path.Combine(eia.LocalFolder, fs.identifier)));
                        if (!downloadedFile.Equals("Failed"))
                        {
                            //Task extract = new Task(() => eia.UnZipping(str, Path.Combine(eia.LocalFolder, fs.identifier)));
                            Task extract = download.ContinueWith((v) => extractedFile = eia.UnZipping(downloadedFile, Path.Combine(eia.LocalFolder, fs.identifier)));
                            Task dataparse = extract.ContinueWith((v) => eia.ParsingData(extractedFile, fs.identifier));

                            //taskList.Add(Task.Factory.StartNew(() => handler.Download(Path.Combine(eia.LocalFolder, fs.identifier))));
                            //Task s = Task.Run(() => eia.DataDownload(fs));
                            //result = s.IsCompleted | s.IsCompleted;
                            //taskList.Add(s);
                            downloadlist.Add(download);
                            operationList.Add(extract);
                            operationList.Add(dataparse);
                        }
                        
                        if (downloadlist.Count >= 3)
                        {
                            int index = Task.WaitAny(downloadlist.ToArray());
                            downloadlist.Remove(Task.CompletedTask);
                        }

                        if(operationList.Count>=5)
                        {
                            Task.WaitAny(operationList.ToArray());
                            operationList.Remove(Task.CompletedTask);
                        }
                        if (Count++ % 3 == 0)
                        {
                            Task.WaitAll(downloadlist.ToArray());
                            downloadlist.Clear();
                        }
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    //Console.WriteLine(s.IsCompleted);
                }

                Task.WaitAll(operationList.ToArray());
                //Task t = Task.Run(() => eia.DataDownload(dataList));
                //t.Wait();

                Console.WriteLine("all Done");

                //If there is no update, stop application.
                Console.ReadLine();
                //Console.ReadLine();
            }
            else
            {
                //If the mainfest file downloaded failed, do something else other than parsing.
            }
        }

        private static EIAUpdater Initializer()
        {
            StreamReader sr = new StreamReader("Config.json");
            EIAUpdater e = JsonConvert.DeserializeObject<EIAUpdater>(sr.ReadToEnd());
            return e;
        }

        private bool GetManifest()
        {
            //Console.WriteLine("Current location: " + System.IO.Directory.GetCurrentDirectory().ToString());
            using (var client = new WebClient())
            {
                try
                {
                    //client.DownloadFile(Manifest, LocalFolder);
                    string strLocalName = Path.Combine(LocalFolder, LocalFileName);
                    if (!File.Exists(strLocalName))
                    {
                        client.DownloadFile(Manifest, strLocalName);
                    }
                }
                catch (WebException we)
                {
                    Console.WriteLine(we.Message.ToString());
                    return false;
                }
                finally
                {
                    client.Dispose();
                }
            }
            return true;
        }

        private List<FileSummary> Parsing()
        {
            List<FileSummary> summary = new List<FileSummary>();
            MongoAgent conn = getConn();
            //string strManifest = "C:\\Quantitative_Finance\\DataGrabing\\manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";

            StreamReader sr = new StreamReader(Path.Combine(LocalFolder, LocalFileName));
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
                List<BsonDocument> list = conn.readCollection(ManifestCollection, query);
                if (list.Count == 0)
                {
                    //If there is no record of such identifier, it means this is a new file type. It needs to be downloaded and insert into database.
                    summary.Add(fs);
                    conn.InsertCollectionAsync(ManifestCollection, BsonDocument.Parse(token.First.ToString()));
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
                        //conn.replaceCollection(ManifestCollection, query, doc);
                        conn.UpdateCollectionAsync(ManifestCollection, query, doc);
                    }
                }

                //summary.Add(fs);

                //conn.insertCollection("EIA_Manifest", token);
            }

            return summary;
        }

        private MongoAgent getConn()
        {
            //var a = MongoUrl.Create("mongodb://[hejia:Hejia_68425291]@localhost:27017/admin").DatabaseName;
            MongoCredential credential = MongoCredential.CreateCredential("admin", UserName, Password);
            MongoClientSettings clientSettings = new MongoClientSettings
            {
                Server = new MongoServerAddress(MongoHost, MongoDBPort),
                Credential = credential,
                //ConnectTimeout = new TimeSpan(0, 1, 0),
                //SocketTimeout = new TimeSpan(0, 1, 0),
                UseSsl = false
            };
            MongoAgent ma = MongoAgent.getInstance(clientSettings);
            //MongoAgent ma = MongoAgent.getInstance("mongodb://hejia:Hejia_68425291@localhost:27017/admin");
            ma.setDatabase(MongoDB);
            return ma;
        }

        private string UnZipping(string zipFile, string extractFolder)
        {
            Console.WriteLine("Start extracting:" + zipFile);
            //System.IO.Compression.
            try
            {
                ZipFile.ExtractToDirectory(zipFile, extractFolder);

                string[] extracted = Directory.GetFiles(extractFolder, "*.txt");
                Uri uri = new Uri(zipFile);
                string strArchive = Path.Combine(new string[] { extractFolder, "Archive", uri.Segments.GetValue(uri.Segments.Length - 1).ToString().Replace(".zip", DateTime.Now.ToString("yyyyMMdd") + ".zip") });
                File.Move(zipFile, strArchive);
                //File.Move(zipFile, zipFile.Replace(".zip", DateTime.Now.ToString("yyyyMMdd") + ".zip"));

                Console.WriteLine("Finish extracting:" + zipFile);
                return extracted[0];
            }
            catch(Exception e)
            {
                throw e;
            }
        }

        private void ParsingData(string DataFile, string Identifier)
        {
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss")+ "|Start parsing data of " + DataFile);
            int Count = 0;
            List<BsonDocument> documents = new List<BsonDocument>();
            StreamReader reader = new StreamReader(DataFile);
            try
            {
                string str = "";
                MongoAgent conn = getConn();
                while (!reader.EndOfStream)
                {
                    str = reader.ReadLine();
                    JObject obj = (JObject)JsonConvert.DeserializeObject(str);
                    BsonDocument bdoc = BsonDocument.Parse(str);
                    //conn.InsertCollection(Identifier, bdoc);
                    documents.Add(bdoc);

                    if (documents.Count == 1000)
                    {
                        conn.InsertCollection(Identifier, documents);
                        documents.Clear();
                    }

                    Count++;
                }
                if (documents.Count > 0)
                    conn.InsertCollection(Identifier, documents);
            }
            catch(Exception e)
            {
                throw e;
            }
            finally
            {
                reader.Dispose();
                File.Delete(DataFile);
            }
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Parsing " + Count + " sets of data of " + Identifier + " has done!");
        }

    }
}

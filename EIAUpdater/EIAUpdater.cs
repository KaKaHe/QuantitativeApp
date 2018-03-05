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
            EIAUpdater eia = Initializer();

            if (eia.GetManifest())
            {
                //If the manifest file is downloaded successfully, continue doing parsing.
                List<FileSummary> dataList = eia.Parsing();
                List<Task> processList = new List<Task>();

                foreach (FileSummary fs in dataList)
                {
                    try
                    {
                        Task process = Task.Factory.StartNew(() => eia.ProcessDataFiles(fs));
                        processList.Add(process);

                        if (processList.Count >= 3)
                        {
                            Task.WaitAny(processList.ToArray());
                            processList.Remove(Task.CompletedTask);
                        }
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }

                Task.WaitAll(processList.ToArray());

                Console.WriteLine("all Done");

                //If there is no update, stop application.
                //Console.ReadLine();
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
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Start getting today's manifest");
            //Console.WriteLine("Current location: " + System.IO.Directory.GetCurrentDirectory().ToString());
            using (var client = new WebClient())
            {
                try
                {
                    //client.DownloadFile(Manifest, LocalFolder);
                    string strLocalName = Path.Combine(LocalFolder, LocalFileName);
                    //If DataGrabing folder doesn't exist, create it.
                    if (!Directory.Exists(LocalFolder))
                    {
                        Directory.CreateDirectory(LocalFolder);
                    }

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
                catch (Exception e)
                {
                    Console.WriteLine(e.Message.ToString());
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
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Start parsing today's manifest");
            List<FileSummary> summary = new List<FileSummary>();
            MongoAgent conn = getConn();

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
                        doc.SetElement(new BsonElement("_id", old._id));
                        //conn.replaceCollection(ManifestCollection, query, doc);
                        conn.UpdateCollectionAsync(ManifestCollection, query, doc);
                    }
                }
            }

            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Finish parsing today's manifest");
            return summary;
        }

        private MongoAgent getConn()
        {
            //var a = MongoUrl.Create("mongodb://[hejia:Hejia_68425291]@localhost:27017/admin").DatabaseName;
            //MongoCredential credential = MongoCredential.CreateCredential("admin", UserName, Password);
            MongoCredential credential = MongoCredential.CreateCredential(MongoDB, UserName, Password);
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

        private void ProcessDataFiles(FileSummary fs)
        {
            try
            {
                FileHandler handler = new FileHandler(fs.accessURL);
                string downloadedfile = handler.DownloadHTTPClient(Path.Combine(LocalFolder, fs.identifier));

                if (!String.IsNullOrEmpty(downloadedfile) && !downloadedfile.Equals("Failed"))
                {
                    System.Threading.Thread.Sleep(5000);
                    string extractedFile = UnZipping(downloadedfile, Path.Combine(LocalFolder, fs.identifier));

                    if (!String.IsNullOrEmpty(extractedFile))
                    {
                        ParsingData(extractedFile, fs.identifier);
                    }
                }
            }
            catch(Exception E)
            {
                throw E;
            }
        }

        private string UnZipping(string zipFile, string extractFolder)
        {
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Start extracting:" + zipFile);

            try
            {
                File.GetAccessControl(zipFile);
                ZipFile.ExtractToDirectory(zipFile, extractFolder);

                string[] extracted = Directory.GetFiles(extractFolder, "*.txt");
                Uri uri = new Uri(zipFile);
                string strArchive = Path.Combine(new string[] { extractFolder, "Archive", uri.Segments.GetValue(uri.Segments.Length - 1).ToString().Replace(".zip", DateTime.Now.ToString("yyyyMMdd") + ".zip") });
                File.Move(zipFile, strArchive);
                //File.Move(zipFile, zipFile.Replace(".zip", DateTime.Now.ToString("yyyyMMdd") + ".zip"));

                Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Finish extracting:" + zipFile);
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
                Console.WriteLine(e.Message);
                Console.WriteLine(e);
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

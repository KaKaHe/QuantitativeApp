using EIAUpdater.Database;
using EIAUpdater.Handler;
using EIAUpdater.Model;
using log4net;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
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
        public bool DebugMode = false;
        public string LocalFileName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";
        //public event AsyncCompletedEventHandler DownloadCompleted;
        public static ILog logger = LogManager.GetLogger(typeof(EIAUpdater));
        static void Main(string[] args)
        {
            //log4net.Config.BasicConfigurator.Configure();
            //logger = LogManager.GetLogger(typeof(EIAUpdater));
            logger.Info("Start updating EIA's data for today");
            EIAUpdater eia = Initializer();
            Configurations config = ReadConfig();
            ManifestHandler manifest = new ManifestHandler(config);

            //if (eia.GetManifest())
            if (manifest.Download())
            {
                //If the manifest file is downloaded successfully, continue doing parsing.
                //List<DataSet> dataList = eia.ParsingManifest();
                List<DataSet> dataList = manifest.Parsing(MongoAgent.GetInstance(config));
                List<Task> processList = new List<Task>();
                List<Task> completelist = new List<Task>();

                foreach (DataSet dataset in dataList)
                {
                    try
                    {
                        FileHandler handler = new FileHandler(dataset.accessURL);
                        Console.WriteLine(processList.Count);
                        if (processList.Count >= 3)
                        {
                            Task.WaitAny(processList.ToArray());
                            processList.ForEach(a =>
                            {
                                if (a.Status.Equals(TaskStatus.RanToCompletion))
                                {
                                    completelist.Add(a);
                                }
                            });
                            foreach (Task a in completelist)
                            {
                                processList.Remove(a);
                            }
                            completelist.Clear();
                        }
                        Task process = Task.Factory.StartNew(() => eia.ProcessDataFiles(dataset));
                        processList.Add(process);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }

                Task.WaitAll(processList.ToArray());
                
                logger.Info("All updated data had been processed for today.");

                //If there is no update, stop application.
                //Console.ReadLine();
                //Console.ReadLine();
            }
            else
            {
                //If the mainfest file downloaded failed, do something else other than parsing.
                logger.Error("Loading manifest failed of day " + DateTime.UtcNow.ToString("yyyyMMdd"));
            }
        }

        [Obsolete]
        private static EIAUpdater Initializer()
        {
            StreamReader sr = new StreamReader("Config.json");
            EIAUpdater e = JsonConvert.DeserializeObject<EIAUpdater>(sr.ReadToEnd());
            logger.Info("Configuration file read successfully.");
            return e;
        }

        private static Configurations ReadConfig()
        {
            StreamReader sr = new StreamReader("Config.json");
            Configurations config = JsonConvert.DeserializeObject<Configurations>(sr.ReadToEnd());
            logger.Info("Configuration file read successfully.");
            return config;
        }

        private bool GetManifest()
        {
            logger.Info("Start downloading today's manifest");
            if (File.Exists(Path.Combine(LocalFolder, LocalFileName)))
            {
                File.Move(Path.Combine(LocalFolder, LocalFileName), Path.Combine(LocalFolder, string.Concat(LocalFileName, DateTime.UtcNow.ToString(".yyyyMMddHHmmss"))));
            }
            FileHandler handler = new FileHandler(Manifest);
            Task<string> task = handler.DownloadWebRequest(LocalFolder, LocalFileName);
            Task.WaitAll();
            string manifest = task.Result;

            if(!string.IsNullOrEmpty(manifest) && !manifest.Equals("Failed"))
                return true;

            return false;
        }

        private List<DataSet> ParsingManifest()
        {
            logger.Info("Start parsing today's manifest");
            List<DataSet> summary = new List<DataSet>();
            MongoAgent conn = GetConn();

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

        private MongoAgent GetConn()
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
            MongoAgent ma = MongoAgent.GetInstance(clientSettings);
            //MongoAgent ma = MongoAgent.getInstance("mongodb://hejia:Hejia_68425291@localhost:27017/admin");
            ma.SetDatabase(MongoDB);
            return ma;
        }

        private void ProcessDataFiles(DataSet fs)
        {
            try
            {
                logger.Info("Task " + fs.identifier + " start.");
                FileHandler handler = new FileHandler(fs.accessURL);
                //string downloadedfile = handler.DownloadHTTPClient(Path.Combine(LocalFolder, fs.identifier));
                //string downloadedfile = handler.DownloadWebClient(Path.Combine(LocalFolder, fs.identifier));

                //Task<string> task = handler.DownloadWebRequest(Path.Combine(LocalFolder, fs.identifier));
                //string downloadedfile = task.Result;
                string downloadedfile = handler.DownloadWebRequest(Path.Combine(LocalFolder, fs.identifier)).Result;

                if (!string.IsNullOrEmpty(downloadedfile) && !downloadedfile.Equals("Failed"))
                {
                    string extractedFile = UnZipping(downloadedfile, Path.Combine(LocalFolder, fs.identifier));

                    if (!String.IsNullOrEmpty(extractedFile))
                    {
                        ParsingData(extractedFile, fs.identifier);
                    }
                }
                logger.Info("Task " + fs.identifier + " end.");
            }
            catch(Exception E)
            {
                logger.Error(E.Message, E);
            }
        }

        /*
         * No downloading
         */
        private void ProcessDataFiles(DataSet fs, string downloadedfile)
        {
            try
            {
                //FileHandler handler = new FileHandler(fs.accessURL);
                //string downloadedfile = handler.DownloadHTTPClient(Path.Combine(LocalFolder, fs.identifier));

                if (!String.IsNullOrEmpty(downloadedfile) && !downloadedfile.Equals("Failed"))
                {
                    //System.Threading.Thread.Sleep(5000);
                    string extractedFile = UnZipping(downloadedfile, Path.Combine(LocalFolder, fs.identifier));

                    if (!String.IsNullOrEmpty(extractedFile))
                    {
                        ParsingData(extractedFile, fs.identifier);
                    }
                }
            }
            catch (Exception E)
            {
                throw E;
            }
        }

        private string UnZipping(string zipFile, string extractFolder)
        {
            logger.Info("Start extracting file " + zipFile);

            try
            {
                File.GetAccessControl(zipFile);
                ZipFile.ExtractToDirectory(zipFile, extractFolder);

                string[] extracted = Directory.GetFiles(extractFolder, "*.txt");
                Uri uri = new Uri(zipFile);
                string strArchive = Path.Combine(new string[] { extractFolder, "Archive", uri.Segments.GetValue(uri.Segments.Length - 1).ToString().Replace(".zip", DateTime.UtcNow.ToString("yyyyMMdd") + ".zip") });
                if (File.Exists(strArchive))
                    File.Move(strArchive, string.Concat(strArchive, ".", DateTime.UtcNow.ToString("yyyyMMddHHmmss")));
                File.Move(zipFile, strArchive);
                //File.Move(zipFile, zipFile.Replace(".zip", DateTime.UtcNow.ToString("yyyyMMdd") + ".zip"));
                logger.Info("File: " + zipFile + " had been archived to :" + strArchive);
                logger.Info("Finish extracting: " + zipFile);
                return extracted[0];
            }
            catch(Exception e)
            {
                throw e;
            }
        }

        private void ParsingData(string DataFile, string Identifier)
        {
            if(DebugMode)
            {
                File.Delete(DataFile);
                logger.Info("[DebugMode] is on, no data will be parsed of " + Identifier);
                return;
            }
            logger.Info("Start parsing data of " + Identifier);
            int BatchSize = Identifier.Equals("EBA") ? 100 : 1000;
            int Count = 0;
            List<BsonDocument> documents = new List<BsonDocument>();
            StreamReader reader = new StreamReader(DataFile);
            try
            {
                string str = "";
                MongoAgent conn = GetConn();
                while (!reader.EndOfStream)
                {
                    str = reader.ReadLine();
                    if (str.Contains("\0"))
                    {
                        logger.Warn(string.Concat("File ", Identifier, ",Line ", (Count + 1).ToString(), " has invalid char."));
                        str = str.Replace("\0", "");
                        if (string.IsNullOrEmpty(str) || string.IsNullOrWhiteSpace(str))
                        {
                            continue;
                        }
                    }
                    JObject obj = (JObject)JsonConvert.DeserializeObject(str);
                    BsonDocument bdoc = BsonDocument.Parse(str);
                    //conn.InsertCollection(Identifier, bdoc);
                    documents.Add(bdoc);

                    if (documents.Count == BatchSize)
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
            StringBuilder sb = new StringBuilder("Finish parsing data of ");
            sb.Append(Identifier);
            sb.Append("(");
            sb.Append(Count.ToString());
            sb.Append(")");
            //logger.Info("Finish parsing data of " + Identifier);
            logger.Info(sb.ToString());
        }

    }
}

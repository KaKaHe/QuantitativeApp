using System;
using System.Net;
using System.IO;
using System.IO.Compression;
using System.ComponentModel;
using System.Threading.Tasks;
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
        private string MongoDB = "Quantitative";
        private string ManifestCollection = "EIA_Manifest";
        private string MongoHost = "localhost";
        private int MongoDBPort = 27017;
        private string Manifest = "http://api.eia.gov/bulk/manifest.txt";
        private string LocalFolder = "C:\\Quantitative_Finance\\DataGrabing";
        private string LocalFileName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";
        //public event AsyncCompletedEventHandler DownloadCompleted;
        static void Main(string[] args)
        {
            //Console.WriteLine("Hello World!");
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
                //foreach (FileSummary fs in dataList)
                //{
                //    try
                //    {
                //        //Start downloading files that have update.
                //        FileHandler handler = new FileHandler(fs.accessURL);
                //        //handler.Download(eia.LocalFolder + "\\" + fs.identifier);
                //        handler.Download(Path.Combine(eia.LocalFolder, fs.identifier));
                //    }
                //    catch(Exception e)
                //    {
                //        Console.WriteLine(e.Message);
                //    }
                //}

                //If there is no update, stop application.
                Console.ReadLine();
                //Console.ReadLine();
            }
            else
            {
                //If the mainfest file downloaded failed, do something else other than parsing.
            }
            //Console.ReadLine();
            //Console.ReadLine();
        }

        private static EIAUpdater Initializer()
        {
            StreamReader sr = new StreamReader("Config.json");
            EIAUpdater e = JsonConvert.DeserializeObject<EIAUpdater>(sr.ReadToEnd());
            return e;
        }

        private bool GetManifest()
        {
            //string strURL = "http://api.eia.gov/bulk/manifest.txt";
            //string strLocation = "C:\\Quantitative_Finance\\DataGrabing\\";
            //string strLocalName = "manifest_" + DateTime.UtcNow.ToString("yyyyMMdd") + ".txt";

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

        //private Task DataDownload(FileSummary fs)
        //{
        //    try
        //    {
        //        //Start downloading files that have update.
        //        FileHandler handler = new FileHandler(fs.accessURL);
        //        //handler.Download(eia.LocalFolder + "\\" + fs.identifier);
        //        string strResult = handler.DownloadAsync(Path.Combine(LocalFolder, fs.identifier));

        //        if(strResult.Equals("Done"))
        //        {
        //            return;
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e.Message);
        //    }
        //    return;
        //}

        //private void DataDownload(List<FileSummary> list)
        //{
        //    List<string> taskList = new List<string>();
        //    foreach (FileSummary fs in list)
        //    {
        //        try
        //        {

        //            //Start downloading files that have update.
        //            //FileHandler handler = new FileHandler(fs.accessURL);
        //            ////handler.Download(eia.LocalFolder + "\\" + fs.identifier);
        //            //Console.WriteLine("Starting " + fs.identifier);
        //            //string result = await handler.Download(Path.Combine(LocalFolder, fs.identifier));
        //            ////string a = await result;
        //            //Console.WriteLine("End " + fs.identifier);
        //            //taskList.Add(result);
        //            Task<string> re = d(fs);
        //            taskList.Add(re.Result);
        //        }
        //        catch (Exception e)
        //        {
        //            Console.WriteLine(e.Message);
        //        }
        //    }
        //    //await taskList;
        //    foreach(string a in taskList)
        //    {
        //        Console.WriteLine(a);
        //    }

        //    //return;
        //}

        //private async Task<string> d(FileSummary fs)
        //{
        //    FileHandler handler = new FileHandler(fs.accessURL);
        //    Console.WriteLine("Starting " + fs.identifier);
        //    string result = handler.Download(Path.Combine(LocalFolder, fs.identifier));
        //    //string a = await result;
        //    Console.WriteLine("End " + fs.identifier);
        //    return result;
        //    //taskList.Add(result);
        //}

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
            MongoClientSettings clientSettings = new MongoClientSettings
            {
                Server = new MongoServerAddress(MongoHost, MongoDBPort),
                UseSsl = false
            };
            MongoAgent ma = MongoAgent.getInstance(clientSettings);
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
            }
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Parsing " + Count + " sets of data of " + Identifier + " has done!");
        }

        [Obsolete]
        private ObjectId getObjectId(ObjectId oldone)
        {
            TimeSpan increment = DateTime.Now - oldone.CreationTime;
            return new ObjectId(oldone.CreationTime, Environment.MachineName.GetHashCode() / 100, (short)System.Diagnostics.Process.GetCurrentProcess().Id, increment.Milliseconds);
        }
    }
}

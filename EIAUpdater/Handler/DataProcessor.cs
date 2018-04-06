using EIAUpdater.Database;
using EIAUpdater.Model;
using log4net;
using MongoDB.Bson;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace EIAUpdater.Handler
{
    public class DataProcessor : IHandler
    {
        public ILog Logger { get; set; } //= LogManager.GetLogger(typeof(DataProcessor));
        public Configurations Config { get; set; }

        public DataProcessor(Configurations config)
        {
            Config = config;
            Logger = LogManager.GetLogger(typeof(DataProcessor));
        }

        public async Task<bool> ProcessingData(DataSetSummary dataSummary)
        {
            try
            {
                Logger.Info("Task " + dataSummary.identifier + " start.");
                FileHandler handler = new FileHandler(dataSummary.accessURL);
                //string downloadedfile = string.Empty;
                int Counter = 3;
                string downloadedfile = await handler.Download(Path.Combine(Config.LocalFolder, dataSummary.identifier));
                while (Counter-- >= 0)
                {
                    //string downloadedfile =  await handler.Download(Path.Combine(configurations.LocalFolder, dataSummary.identifier));

                    if (!string.IsNullOrEmpty(downloadedfile) && !downloadedfile.Equals("Failed"))
                    {
                        string extractedFile = handler.UnZipping(downloadedfile, Path.Combine(Config.LocalFolder, dataSummary.identifier));

                        if (!String.IsNullOrEmpty(extractedFile))
                        {
                            ParsingData(extractedFile, dataSummary.identifier, Config.DebugMode);
                        }
                    }
                    else
                    {
                        if (Counter == 0)
                            break;
                        System.Threading.Thread.Sleep(Config.RetryInterval);
                        Logger.Info("Retry downloading " + dataSummary.identifier);
                        string strIncomplete = Path.Combine(Config.LocalFolder, dataSummary.identifier, dataSummary.accessURL.Substring(dataSummary.accessURL.LastIndexOf("/") + 1));
                        if (File.Exists(strIncomplete))
                            File.Delete(strIncomplete);
                        downloadedfile = handler.Download(Path.Combine(Config.LocalFolder, dataSummary.identifier)).Result;
                        continue;
                    }
                    Logger.Info("Task " + dataSummary.identifier + " end.");
                    return true;
                }
                Logger.Info("Downloading " + dataSummary.identifier + " failed.");
                return false;
            }
            catch (Exception E)
            {
                Logger.Error(E.Message, E);
                return false;
            }
        }

        public void ParsingData(string DataFile, string Identifier, bool DebugMode = false)
        {
            if (DebugMode)
            {
                File.Delete(DataFile);
                Logger.Info("[DebugMode] is on, no data will be parsed of " + Identifier);
                return;
            }
            Logger.Info("Start parsing data of " + Identifier);
            int BatchSize = Identifier.Equals("EBA") ? 100 : 1000;
            int Count = 0;
            List<BsonDocument> documents = new List<BsonDocument>();
            StreamReader reader = new StreamReader(DataFile);
            try
            {
                string str = "";
                MongoAgent conn = MongoAgent.GetInstance(Config);
                while (!reader.EndOfStream)
                {
                    str = reader.ReadLine();
                    if (str.Contains("\0"))
                    {
                        Logger.Warn(string.Concat("File ", Identifier, ",Line ", (Count + 1).ToString(), " has invalid char."));
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
            catch (Exception e)
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
            Logger.Info(sb.ToString());
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

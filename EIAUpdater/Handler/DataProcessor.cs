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

namespace EIAUpdater.Handler
{
    public class DataProcessor
    {
        private static ILog logger = LogManager.GetLogger(typeof(DataProcessor));
        private Configurations configurations;

        public DataProcessor(Configurations config)
        {
            configurations = config;
        }

        public void ProcessingData(DataSetSummary dataSummary)
        {
            try
            {
                logger.Info("Task " + dataSummary.identifier + " start.");
                FileHandler handler = new FileHandler(dataSummary.accessURL);
                //string downloadedfile = string.Empty;

                while (true)
                {
                    string downloadedfile = handler.Download(Path.Combine(configurations.LocalFolder, dataSummary.identifier)).Result;

                    if (!string.IsNullOrEmpty(downloadedfile) && !downloadedfile.Equals("Failed"))
                    {
                        string extractedFile = handler.UnZipping(downloadedfile, Path.Combine(configurations.LocalFolder, dataSummary.identifier));

                        if (!String.IsNullOrEmpty(extractedFile))
                        {
                            ParsingData(extractedFile, dataSummary.identifier, configurations.DebugMode);
                        }
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(100000);
                        logger.Info("Retry downloading " + dataSummary.identifier);
                        continue;
                    }
                    logger.Info("Task " + dataSummary.identifier + " end.");
                    break;
                }
            }
            catch (Exception E)
            {
                logger.Error(E.Message, E);
            }
        }

        public void ParsingData(string DataFile, string Identifier, bool DebugMode = false)
        {
            if (DebugMode)
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
                MongoAgent conn = MongoAgent.GetInstance(configurations);
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
            logger.Info(sb.ToString());
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

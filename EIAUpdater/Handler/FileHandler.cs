using System;
using System.Collections.Generic;
//using System.ComponentModel;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EIAUpdater.Database;
using EIAUpdater.Model;
using log4net;
using Newtonsoft.Json;

namespace EIAUpdater.Handler
{
    public class FileHandler : IHandler
    {
        private string FileURL;
        private string LocalFileName;
        //public event AsyncCompletedEventHandler DownloadCallback;
        public ILog Logger { get; set; } //= LogManager.GetLogger(typeof(FileHandler));
        public Configurations Config { get; set; }
        public FileHandler(string remotePath)
        {
            FileURL = remotePath;
            LocalFileName = FileURL.Substring(remotePath.LastIndexOf("/") + 1);
            Logger = LogManager.GetLogger(typeof(FileHandler));
        }

        public async Task<string> Download(string strLocalPath, string strLocalName = "")
        {
            Logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
            if (!string.IsNullOrEmpty(strLocalName))
                LocalFileName = strLocalName;
            HttpWebRequest request = null;
            HttpWebResponse response = null;
            FileStream filestream = null;
            Stream stream = null;
            long FullLength = -1;
            int ByteCounter = 0;
            int BufferSize = 2048000;
            string strLocalFile = Path.Combine(strLocalPath, LocalFileName);
            try
            {
                if (!Directory.Exists(strLocalPath))
                {
                    Directory.CreateDirectory(strLocalPath);
                    Directory.CreateDirectory(Path.Combine(strLocalPath, "Archive"));
                }
                //string strLocalFile = Path.Combine(strLocalPath, LocalFileName);
                
                //do
                //{
                ByteCounter = 0;
                request = (HttpWebRequest)WebRequest.Create(FileURL);
                using (response = (HttpWebResponse)await request.GetResponseAsync())
                {
                    FullLength = response.ContentLength;
                    Logger.Info((new StringBuilder("Get Response of ")).Append(FullLength).Append(" bytes of ").Append(LocalFileName));
                    //logger.Info();
                    //logger.Info("Get Response of " + FullLength + " bytes of " + );
                    if (FullLength < BufferSize)
                        BufferSize = (int)FullLength;
                    using (stream = response.GetResponseStream())
                    {
                        byte[] buf = new byte[BufferSize];
                        int read = 0;
                        filestream = new FileStream(strLocalFile, FileMode.Create, FileAccess.Write, FileShare.None);
                        while ((read = stream.Read(buf, 0, BufferSize)) != 0)
                        {
                            ByteCounter += read;
                            filestream.Write(buf, 0, read);
                            //if (ByteCounter / (double)FullLength > 0.5)
                            //    logger.Info("half of file " + strLocalName + " downloaded!.");
                        }
                    }
                    Logger.Info(ByteCounter.ToString() + " bytes had been written to " + LocalFileName);
                    if (ByteCounter != FullLength)
                    {
                        Logger.Warn(LocalFileName + " is incomplete, restarting downloading again.");
                        //filestream.Flush();
                        //filestream.Close();
                        //File.Delete(strLocalFile);
                        //Thread.Sleep(10000);
                        return "Failed";
                    }
                }
                //} while (ByteCounter != FullLength);

                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (Exception e)
            {
                Logger.Info("Downloading failed. " + Path.Combine(strLocalPath, LocalFileName));
                Logger.Error(e.Message, e);
                //if (File.Exists(strLocalFile))
                //    File.Delete(strLocalFile);
                return "Failed";
            }
            finally
            {
                response.Dispose();
                response.Close();
                filestream.Flush();
                filestream.Close();
                Logger.Info("Finish downloading " + LocalFileName);
            }
        }

        public string UnZipping(string zipFile, string extractFolder)
        {
            Logger.Info("Start extracting file " + zipFile);

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
                Logger.Info("File: " + zipFile + " had been archived to :" + strArchive);
                Logger.Info("Finish extracting: " + zipFile);
                return extracted[0];
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public static T ReadJsontoObject<T>(string Path)
        {
            StreamReader sr = new StreamReader(Path);
            T config = JsonConvert.DeserializeObject<T>(sr.ReadToEnd());
            return config;
        }

        [Obsolete]
        public string DownloadWebClient(string strLocalPath)
        {
            Logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
            WebClient client = null;

            try
            {
                using (client = new WebClient())
                {
                    if (!Directory.Exists(strLocalPath))
                    {
                        Directory.CreateDirectory(strLocalPath);
                        Directory.CreateDirectory(Path.Combine(strLocalPath, "Archive"));
                    }
                    string strLocalFile = Path.Combine(strLocalPath, LocalFileName);
                    //request.DownloadFileCompleted += new AsyncCompletedEventHandler(DownloadCallback);
                    //request.DownloadFileAsync(new Uri(FileURL), strLocalFile);
                    client.DownloadFile(FileURL, strLocalFile);
                }
                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                Logger.Info("Finish downloading " + LocalFileName);
            }
        }

        [Obsolete]
        public string DownloadHTTPClient(string strLocalPath)
        {
            Logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
            HttpClient client = null;
            FileStream stream = null;

            try
            {
                using (client = new HttpClient())
                {
                    if (!Directory.Exists(strLocalPath))
                    {
                        Directory.CreateDirectory(strLocalPath);
                        Directory.CreateDirectory(Path.Combine(strLocalPath, "Archive"));
                    }
                    string strLocalFile = Path.Combine(strLocalPath, LocalFileName);

                    Logger.Info("Send HttpRequest");
                    HttpContent content = client.GetAsync(FileURL).Result.Content;
                    Logger.Info("Got HttpResponse content");
                    stream = new FileStream(strLocalFile, FileMode.Create, FileAccess.Write, FileShare.None);
                    content.CopyToAsync(stream);
                    Logger.Info("Put them into Stream.");
                    content.Dispose();
                }
                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                stream.Close();
                Logger.Info("Finish downloading " + LocalFileName);
            }
        }

        public void ParsingData(string DataFile, string Identifier, bool DebugMode = false)
        {
            throw new NotImplementedException();
        }

        public List<DataSetSummary> ParsingData(MongoAgent conn)
        {
            throw new NotImplementedException();
        }
    }
}

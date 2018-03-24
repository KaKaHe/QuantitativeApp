using System;
//using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace EIAUpdater.Handler
{
    public class FileHandler
    {
        private string FileURL;
        private string LocalFileName;
        //public event AsyncCompletedEventHandler DownloadCallback;
        public static ILog logger = LogManager.GetLogger(typeof(FileHandler));
        public FileHandler(string remotePath)
        {
            FileURL = remotePath;
            LocalFileName = FileURL.Substring(remotePath.LastIndexOf("/") + 1);
        }

        public string DownloadWebClient(string strLocalPath)
        {
            logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
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
            catch(Exception e)
            {
                logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                logger.Info("Finish downloading " + LocalFileName);
            }
        }

        public string DownloadHTTPClient(string strLocalPath)
        {
            logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
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
                    
                    logger.Info("Send HttpRequest");
                    HttpContent content = client.GetAsync(FileURL).Result.Content;
                    logger.Info("Got HttpResponse content");
                    stream = new FileStream(strLocalFile, FileMode.Create, FileAccess.Write, FileShare.None);
                    content.CopyToAsync(stream);
                    logger.Info("Put them into Stream.");
                    content.Dispose();
                }
                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (Exception e)
            {
                logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                stream.Close();
                logger.Info("Finish downloading " + LocalFileName);
            }
        }

        public async Task<string> DownloadWebRequest(string strLocalPath, string strLocalName = "")
        {
            logger.Info("Start downloading " + FileURL + " to " + strLocalPath);
            if (!string.IsNullOrEmpty(strLocalName))
                LocalFileName = strLocalName;
            HttpWebRequest request = null;
            HttpWebResponse response = null;
            FileStream filestream = null;
            Stream stream = null;
            long FullLength = -1;
            int ByteCounter = 0;
            try
            {
                if (!Directory.Exists(strLocalPath))
                {
                    Directory.CreateDirectory(strLocalPath);
                    Directory.CreateDirectory(Path.Combine(strLocalPath, "Archive"));
                }
                string strLocalFile = Path.Combine(strLocalPath, LocalFileName);
                
                do
                {
                    ByteCounter = 0;
                    request = (HttpWebRequest)WebRequest.Create(FileURL);
                    using (response = (HttpWebResponse)await request.GetResponseAsync())
                    {
                        FullLength = response.ContentLength;
                        logger.Info("Get Response of " + FullLength + " bytes");
                        using (stream = response.GetResponseStream())
                        {
                            byte[] buf = new byte[10240];
                            int read = 0;
                            filestream = new FileStream(strLocalFile, FileMode.Create, FileAccess.Write, FileShare.None);
                            while ((read = stream.Read(buf, 0, 10240)) != 0)
                            {
                                ByteCounter += read;
                                filestream.Write(buf, 0, read);
                            }
                        }
                        logger.Info(ByteCounter.ToString() + " bytes had been written to " + LocalFileName);
                        if (ByteCounter < FullLength)
                        {
                            logger.Warn(LocalFileName + " is incomplete, restarting downloading again.");
                            filestream.Flush();
                            filestream.Close();
                            File.Delete(strLocalFile);
                            Thread.Sleep(10000);
                        }
                    }
                } while (ByteCounter != FullLength);

                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (Exception e)
            {
                logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                response.Dispose();
                response.Close();
                filestream.Close();
                logger.Info("Finish downloading " + LocalFileName);
            }
        }
    }
}

using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using log4net;

namespace EIAUpdater
{
    public class FileHandler
    {
        private string FileURL;
        private string LocalFileName;
        //public event AsyncCompletedEventHandler DownloadCallback;
        public static ILog logger = LogManager.GetLogger(typeof(FileHandler));
        public FileHandler(string strPath)
        {
            FileURL = strPath;
            LocalFileName = FileURL.Substring(strPath.LastIndexOf("/") + 1);
        }

        public string DownloadWebClient(string strLocalPath)
        {
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Start downloading " + strLocalPath);
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
                    //client.DownloadFileCompleted += new AsyncCompletedEventHandler(DownloadCallback);
                    //client.DownloadFileAsync(new Uri(FileURL), strLocalFile);
                    client.DownloadFile(FileURL, strLocalFile);
                }
                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (WebException we)
            {
                Console.WriteLine(we.Message);
                return "Failed";
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                Console.WriteLine("Finish downloading " + strLocalPath);
            }
            //Task<string> t = new Task<string>(() => { return "Done"; });
            //string a = "done";
            //return t;
        }

        public string DownloadHTTPClient(string strLocalPath)
        {
            //Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Start downloading " + FileURL + " to " + strLocalPath);
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
                    
                    HttpContent content = client.GetAsync(FileURL).Result.Content;
                    stream = new FileStream(strLocalFile, FileMode.Create, FileAccess.Write, FileShare.None);
                    content.CopyToAsync(stream);
                    content.Dispose();
                }
                return Path.Combine(strLocalPath, LocalFileName);
            }
            catch (WebException we)
            {
                //Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"));
                //Console.WriteLine(we.Message);
                logger.Error(we.Message, we);
                return "Failed";
            }
            catch (Exception e)
            {
                //Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"));
                //Console.WriteLine(e);
                logger.Error(e.Message, e);
                return "Failed";
            }
            finally
            {
                client.Dispose();
                stream.Close();
                //Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "|Finish downloading " + LocalFileName);
                logger.Info("Finish downloading " + LocalFileName);
            }
        }

        //public void DownloadCallback(object sender, AsyncCompletedEventArgs e)
        //{
        //    Console.WriteLine("Downloading completed.");
        //    Directory.CreateDirectory("C:\\Quantitative_Finance\\DataGrabing\\Completed");
        //}
    }
}

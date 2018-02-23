using System;
using System.ComponentModel;
using System.IO;
using System.Net;

namespace EIAUpdater
{
    public class FileHandler
    {
        private string FileURL;
        private string LocalFileName;
        //public event AsyncCompletedEventHandler DownloadCallback;
        public FileHandler(string strPath)
        {
            FileURL = strPath;
            LocalFileName = FileURL.Substring(strPath.LastIndexOf("/") + 1);
        }

        public string Download(string strLocalPath)
        {
            Console.WriteLine("Start downloading " + strLocalPath);
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
            finally
            {
                client.Dispose();
                Console.WriteLine("Finish downloading " + strLocalPath);
            }
            //Task<string> t = new Task<string>(() => { return "Done"; });
            //string a = "done";
            //return t;
        }

        //public void DownloadCallback(object sender, AsyncCompletedEventArgs e)
        //{
        //    Console.WriteLine("Downloading completed.");
        //    Directory.CreateDirectory("C:\\Quantitative_Finance\\DataGrabing\\Completed");
        //}
    }
}

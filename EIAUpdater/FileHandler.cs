using System.IO;
using System.Collections.Generic;
using System;
using System.Net;
using System.ComponentModel;
using System.Text;
using System.Threading.Tasks;

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
            Console.WriteLine("Strating " + strLocalPath);
            WebClient client = null;
            try
            {
                using (client = new WebClient())
                {
                    if (!Directory.Exists(strLocalPath))
                    {
                        Directory.CreateDirectory(strLocalPath);
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
                throw we;
            }
            finally
            {
                client.Dispose();
                Console.WriteLine("End " + strLocalPath);
            }
            //Task<string> t = new Task<string>(() => { return "Done"; });
            //string a = "done";
            //return t;
        }

        [Obsolete]
        public async Task<string> DownloadAsync(string strLocalPath)
        {
            await Task.Run(() => Download(strLocalPath));
            return "Done";
        }

        public void DownloadCallback(object sender, AsyncCompletedEventArgs e)
        {
            Console.WriteLine("Downloading completed.");
            Directory.CreateDirectory("C:\\Quantitative_Finance\\DataGrabing\\Completed");
        }
    }
}

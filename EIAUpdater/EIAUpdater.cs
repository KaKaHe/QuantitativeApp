using EIAUpdater.Database;
using EIAUpdater.Handler;
using EIAUpdater.Model;
using log4net;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace EIAUpdater
{
    class EIAUpdater
    {
        public static ILog logger;

        static void Main(string[] args)
        {
            if (args == null || args.Length == 0)
            {
                GlobalContext.Properties["LogPath"] = @"C:\EIA_Updater\Logs\";
                logger = LogManager.GetLogger(typeof(EIAUpdater));
                logger.Error("No Configuration file found.");
                return;
            }
            //Configurations config = FileHandler.ReadJsontoObject<Configurations>("Config.json");
            //logger.Info("Configuration file read successfully.");
            try
            {
                Configurations config = FileHandler.ReadJsontoObject<Configurations>(args[0]);
                GlobalContext.Properties["LogPath"] = Path.Combine(config.LogPath, "");
                logger = LogManager.GetLogger(typeof(EIAUpdater));
                logger.Info("Start updating EIA's data for today");
                logger.Info("Read configuration of " + args[0]);
                ManifestHandler manifest = new ManifestHandler(config);

                if (manifest.Download())
                {
                    //If the manifest file is downloaded successfully, continue doing parsing.
                    List<DataSetSummary> dataList = manifest.ParsingData(MongoAgent.GetInstance(config));
                    List<Task> processList = new List<Task>();
                    List<Task> completelist = new List<Task>();

                    foreach (DataSetSummary dataset in dataList)
                    {
                        try
                        {
                            if (processList.Count >= config.ConcurrentThread)
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
                            Task process = Task.Factory.StartNew(() =>
                            {
                                DataProcessor processor = new DataProcessor(config);
                                bool flag = processor.ProcessingData(dataset).Result;
                                if (flag)
                                {
                                    manifest.UpdateManifest(dataset);
                                }
                            });
                            processList.Add(process);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }
                    }

                    Task.WaitAll(processList.ToArray());

                    logger.Info("All updated data had been processed for today.");
                }
                else
                {
                    //If the mainfest file downloaded failed, do something else other than parsing.
                    logger.Error("Loading manifest failed of day " + DateTime.UtcNow.ToString("yyyyMMdd"));
                }
            }
            catch(Exception error)
            {
                logger.Error(error.Message);
            }
        }
    }
}

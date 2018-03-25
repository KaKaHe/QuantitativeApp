namespace EIAUpdater.Model
{
    public class Configurations
    {
        public string MongoHost { get; set; }
        public string MongoDB { get; set; }
        public int MongoDBPort { get; set; }
        public string Manifest { get; set; }
        public string ManifestCollection { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string LocalFolder { get; set; }
        public bool DebugMode { get; set; }
    }
}

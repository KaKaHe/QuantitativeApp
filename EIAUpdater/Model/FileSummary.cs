using MongoDB.Bson;

namespace EIAUpdater.Model
{
    public class FileSummary
    {
        public ObjectId _id;
        public string last_updated;
        public string category_id;
        public string name;
        public string data_set;
        public string identifier;
        public string title;
        public string description;
        public string keyword;
        public string publisher;
        public string person;
        public string mbox;
        public string accessLevel;
        public string accessLevelComment;
        public string accessURL;
        public string webService;
        public string format;
        public string spatial;
        public string temporal;
        public string modified;
        public string token;

        public FileSummary()
        {

        }
    }
}

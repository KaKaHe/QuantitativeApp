using EIAUpdater.Model;
using MongoDB.Bson;
using MongoDB.Driver;
//using Newtonsoft.Json.Linq;
//using System;
using System.Collections.Generic;
using System.Linq;

namespace EIAUpdater.Database
{
    public class MongoAgent
    {
        private static MongoAgent instance = null;
        private static MongoClient client = null;
        private static IMongoDatabase database = null;
        private static IMongoCollection<BsonDocument> collection = null;

        private MongoAgent(MongoClientSettings mcs)
        {
            client = new MongoClient(mcs);
        }

        private MongoAgent(string strConn)
        {
            client = new MongoClient(strConn);
        }

        public static MongoAgent GetInstance(MongoClientSettings clientSettings)
        {
            if (instance == null)
                instance = new MongoAgent(clientSettings);
            return instance;
        }

        public static MongoAgent GetInstance(string strConn)
        {
            if (instance == null)
                instance = new MongoAgent(strConn);
            return instance;
        }

        public static MongoAgent GetInstance(Configurations config)
        {
            if (instance == null)
            {
                MongoCredential credential = MongoCredential.CreateCredential(config.MongoDB, config.UserName, config.Password);
                MongoClientSettings clientSettings = new MongoClientSettings
                {
                    Server = new MongoServerAddress(config.MongoHost, config.MongoDBPort),
                    Credential = credential,
                    UseSsl = false
                };
                instance = GetInstance(clientSettings);
                instance.SetDatabase(config.MongoDB);
            }
            return instance;
        }

        //private IMongoDatabase imd = null;
        public void SetDatabase(string strDB)
        {
            database = client.GetDatabase(strDB);
            //collection = database.GetCollection<BsonDocument>("");
        }

        private void GetCollection(string strCollection)
        {
            //database = client.GetDatabase("Quantitative");
            collection = database.GetCollection<BsonDocument>(strCollection);
            if (collection == null)
            {
                database.CreateCollection(strCollection, new CreateCollectionOptions { AutoIndexId = true });
                collection = database.GetCollection<BsonDocument>(strCollection);
            }
        }

        private void ReleaseCollection()
        {
            collection = null;
        }

        public void InsertCollection(string strCollection, BsonDocument bsonDocument)
        {
            GetCollection(strCollection);
            collection.InsertOne(bsonDocument);
            ReleaseCollection();
        }

        public async void InsertCollectionAsync(string strCollection, BsonDocument bsonDocument)
        {
            GetCollection(strCollection);
            await collection.InsertOneAsync(bsonDocument);
            ReleaseCollection();
        }

        public void InsertCollection(string strCollection, List<BsonDocument> bsonDocument)
        {
            GetCollection(strCollection);
            collection.InsertMany(bsonDocument);
            ReleaseCollection();
        }

        /* Obsoleted function
        [Obsolete]
        public void insertCollection(string strCollection, JToken token)
        {
            //collection = database.GetCollection<BsonDocument>(strCollection);
            //getCollection(strCollection);
            //strData = strData.Substring(11, strData.Length - 14);
            //BsonDocument bd = BsonDocument.Parse(strData);
            //string strData = token.ToString().Replace("AEO.", "AEO-");
            //string strData = encodeDotDollar(token.ToString());
            //string strData = token.First.ToString();
            //JObject obj = (JObject)JToken.ReadFrom(new JsonTextReader(new StringReader(token.ToString())));
            //strData = "{" + strData + "}";
            //BsonDocument bson = token.ToBsonDocument();
            //BsonDocument bson = BsonDocument.Parse(strData);
            //JsonConvert.SerializeObject(token.ToJson());
            //BsonDocument doc = BsonDocument.Parse(JsonConvert.SerializeObject(token.ToString()));
            //BsonDocument bd = null;
            //BsonDocument.TryParse(strData, out bd);
            //collection.InsertOne(bson);
            //string a = JsonConvert.SerializeObject(token.ToString());

            //JsonSerializer js = new JsonSerializer();
            //MemoryStream ms = new MemoryStream();
            //BsonWriter writer = new BsonWriter(ms);
            //js.Serialize(writer, token);

            //string a = Convert.ToBase64String(ms.ToArray());
            //byte[] b = Convert.FromBase64String(a);

            //BsonDocument doc = BsonSerializer.Deserialize<BsonDocument>(b);

            //collection.InsertOne(bson);
            //releaseCollection();
        }
        */

        public List<BsonDocument> ReadCollection(string strCollection)
        {
            //collection = database.GetCollection<BsonDocument>(strCollectionName);
            GetCollection(strCollection);
            var myObj = collection.Find(new BsonDocument()).ToCursor();
            ReleaseCollection();
            //List<BsonDocument> list = myObj.ToList();
            return myObj.ToList();

            //var count = collection.Count(new BsonDocument("identifier", "AEO.2014"));
            //BsonDocument bdd = null;

            //foreach (BsonDocument bdd in myObj.ToEnumerable())
            //{
            //    //bdd = ddd.GetElement(1).ToBsonDocument();
            //}

            //doc.Contains(new BsonDocument());
            //d2.ToString();
        }

        public List<BsonDocument> ReadCollection(string strCollection, BsonDocument document, FindOptions options = null)
        {
            collection = database.GetCollection<BsonDocument>(strCollection);
            IFindFluent<BsonDocument,BsonDocument> obj= collection.Find(document, options);
            ReleaseCollection();
            return obj.ToList();
        }

        public async void UpdateCollectionAsync(string strCollection, BsonDocument filter, BsonDocument document, UpdateOptions options = null)
        {
            GetCollection(strCollection);
            BsonDocument bDoc = new BsonDocument("$set", document);
            await collection.UpdateOneAsync(filter, bDoc, options);
            ReleaseCollection();
        }

        public void UpdateCollection(string strCollection, BsonDocument filter, BsonDocument document, UpdateOptions options = null)
        {
            GetCollection(strCollection);
            BsonDocument bDoc = new BsonDocument("$set", document);
            collection.UpdateOne(filter, bDoc, options);
            ReleaseCollection();
        }

        public void ReplaceCollection(string strCollection, BsonDocument query, BsonDocument document, UpdateOptions options = null)
        {
            GetCollection(strCollection);
            collection.ReplaceOne(query, document, options);
            ReleaseCollection();
        }

        public void UpsertCollection(string strCollection, BsonDocument query, BsonDocument document, UpdateOptions option = null)
        {
            GetCollection(strCollection);
            option = new UpdateOptions { IsUpsert = true };
            collection.ReplaceOne(query, document, option);
            ReleaseCollection();
        }

        public async void ReplaceCollectionAsync(string strCollection, BsonDocument query, BsonDocument document, UpdateOptions options = null)
        {
            GetCollection(strCollection);
            await collection.ReplaceOneAsync(query, document, options);
            ReleaseCollection();
        }

        public void DeleteCollection()
        {

        }

        //private string encodeDotDollar(string input)
        //{
        //    return input.Replace("\\","\\\\").Replace(".", "\\u002e").Replace("$", "\\u0024");
        //}

        //private string decodeDotDollar(string input)
        //{
        //    return input.Replace("\u002e", ".").Replace("\\u0024", "$").Replace("\\\\", "\\");
        //}
        
    }
}

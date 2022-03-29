using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace IMDBMongo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("IMDB MONGO TEST");

            var client = new MongoClient();

            var databases = client.ListDatabases().ToList();

            //foreach ( var database in databases)
            //{
            //    Console.WriteLine(database);
            //}

            var IMDB = client.GetDatabase("IMDB");

            var collections = IMDB.ListCollections().ToList();

            //foreach (var collection in collections)
            //{
            //    Console.WriteLine(collection);
            //}

            var titlesBasic = IMDB.GetCollection<BsonDocument>("TitlesBasic");

            var titlesList = titlesBasic.Find("{}").Limit(10).ToList();

            //var joinedList = titlesBasic
            //    .Aggregate().
            //    Limit(10).
            //    Lookup("TitlePrincipals"  /*Database i want to join*/, "tconst" /*Foreign key in TitleBasic*/
            //    , "tconst"  /*Foreign key in principals*/, "principals"  /*Name of the temporary join*/)
            //    .ToList();

            //All persons involed in a movie

            var joinedList = titlesBasic
               .Aggregate().
               Limit(10).
               Lookup("TitlePrincipals"  /*Database i want to join*/, "tconst" /*Foreign key in TitleBasic*/
               , "tconst"  /*Foreign key in principals*/, "personInMovie"  /*Name of the temporary join*/)
               .Lookup("NamesBasic", "primaryName", "primaryName", "ss")
               .ToList();


            foreach (var title in titlesList)
            {
                Console.WriteLine(title);
            }




        }
    }
}

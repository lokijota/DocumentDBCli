namespace DocDbCli
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Net;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using System.IO;
    using Newtonsoft.Json.Linq;

    class Program
    {
        private string _endpointUri = Properties.Settings.Default.EndpointUri;
        private string _primaryKey = Properties.Settings.Default.AccessKey;
        private DocumentClient _client;
        private enum OperationType { NONE, TESTCONNECTION, CREATEDATABASE, CREATECOLLECTION, INSERTDOCUMENT };

        static void Main(string[] args)
        {
            OperationType operation = OperationType.NONE;
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            #region Parse parameters

            if(args == null || args.Length<1)
            {
                PrintUsage("Your forgot your command line parameters. Please try again.");
                return;
            }

            if (args[0] == "t")
            {
                operation = OperationType.TESTCONNECTION;
            }
            else if (args[0] == "cd")
            {
                if (args.Length < 2)
                {
                    PrintUsage("Missing parameters for cd option. Please try again.");
                    return;
                }
                operation = OperationType.CREATEDATABASE;
                parameters.Add("newdbname", args[1]);
            }
            else if (args[0] == "cc")
            {
                if (args.Length < 3)
                {
                    PrintUsage("Missing parameters for cc option. Please try again.");
                    return;
                }
                operation = OperationType.CREATECOLLECTION;
                parameters.Add("newdbname", args[1]);
                parameters.Add("newcolname", args[2]);

                int throughputInRUs = 400; // default value
                if (args.Length == 4)
                {
                    bool isInt = int.TryParse(args[3], out throughputInRUs);
                    if(!isInt || throughputInRUs % 100 != 0 || throughputInRUs < 400)
                    {
                        PrintUsage("Throughput parameter must be an integer multiple of 100 and >= 400");
                        return;
                    }
                }
                parameters.Add("throughput", throughputInRUs.ToString());

            }
            else if (args[0] == "i")
            {
                if (args.Length < 4)
                {
                    PrintUsage("Missing parameters for i option. Please try again.");
                    return;
                }

                if (!File.Exists(args[3]))
                {
                    PrintUsage(string.Format("File {0} doesn't exit. Fix and retry.", args[3]));
                    return;
                }

                operation = OperationType.INSERTDOCUMENT;
                parameters.Add("newdbname", args[1]);
                parameters.Add("newcolname", args[2]);
                parameters.Add("jsonfile", args[3]);
            }

            #endregion

            Program p = new Program();

            switch (operation)
            {
                case OperationType.TESTCONNECTION:
                    int numberDbs = p.CreateDocumentDbClient().GetAwaiter().GetResult();
                    Console.WriteLine("Test connection ok to {0} which has {1} database(s)", p._endpointUri, numberDbs);
                    break;
                case OperationType.CREATEDATABASE:
                    p.CreateDatabaseIfNotExists(parameters["newdbname"]).Wait();
                    break;
                case OperationType.CREATECOLLECTION:
                    p.CreateDocumentCollectionIfNotExists(parameters["newdbname"], parameters["newcolname"], int.Parse(parameters["throughput"])).Wait();
                    break;
                case OperationType.INSERTDOCUMENT:
                    StreamReader sr = new StreamReader(parameters["jsonfile"]);
                    string jsonDocument = sr.ReadToEnd();
                    JObject jsonObject = JObject.Parse(jsonDocument);
                    p.CreateDocumentIfNotExists(parameters["newdbname"], parameters["newcolname"], jsonObject).Wait();
                    break;
            }
        }

        private async Task CreateDatabaseIfNotExists(string databaseName)
        {
            await CreateDocumentDbClient();

            try
            {
                await _client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
                WriteToConsoleAndPromptToContinue("Found already existing database '{0}'", databaseName);
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await _client.CreateDatabaseAsync(new Database { Id = databaseName,  });
                    WriteToConsoleAndPromptToContinue("Created new database '{0}'", databaseName);
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task CreateDocumentCollectionIfNotExists(string databaseName, string collectionName, int throughput)
        {
            await CreateDocumentDbClient();

            try
            {
                await _client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName));
                WriteToConsoleAndPromptToContinue("Found already existing document collection '{0}'", collectionName);
            }
            catch (DocumentClientException de)
            {
                // If the document collection does not exist, create a new collection
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    DocumentCollection collectionInfo = new DocumentCollection();
                    collectionInfo.Id = collectionName;

                    // Configure collections for maximum query flexibility including string range queries.
                    collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                    // Here we create a collection with 400 RU/s.
                    await _client.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(databaseName),
                        collectionInfo, 
                        new RequestOptions { OfferThroughput = throughput } );

                    WriteToConsoleAndPromptToContinue("Created new document collection '{0}'", collectionName);
                }
                else
                {
                    throw;
                }
            }
        }

        private async Task CreateDocumentIfNotExists(string databaseName, string collectionName, Object jsonObject)
        {
            await CreateDocumentDbClient();

            Document d = await _client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), jsonObject);
            WriteToConsoleAndPromptToContinue("Created document with id {0}", d.Id);
        }

        private async Task<int> CreateDocumentDbClient()
        {
            _client = new DocumentClient(new Uri(_endpointUri), _primaryKey);
            // this will throw if there is an error
            return _client.CreateDatabaseQuery().ToList().Count;
        }

        private static void PrintUsage(string errorMessage = null)
        {
            if (errorMessage != null)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(errorMessage);
                Console.ForegroundColor = ConsoleColor.Gray;
            }

            Console.WriteLine("Usage:");
            Console.WriteLine("DocDbCli t                               Test connection using settings in configuration file");
            Console.WriteLine("DocDbCli cd dbname                       Create database named 'dbname'");
            Console.WriteLine("DocDbCli cc dbname colname [throughput, 400 by default] Create collection named 'colname' in database 'dbname'");
            Console.WriteLine("DocDbCli i dbname colname filename.json  Insert contents of json file into collection 'colname' in database 'dbname'");
        }

        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }
    }
}

using System;
using System.Diagnostics;
using System.IO;
using Google.Datastore.V1;
using Google.Storage.V1;

namespace GcloudDotnetBenchmark
{
    public static class Config
    {
        public const string Project = "ProjectName";

        public const string DatastoreNamespace = "DatastoreNamespace";
        public const string DatastoreKind = "Perf";
        public const string DatastoreKey = "PerfTestEntityKey";

        public const string StorageBucketName = "TestBucketName";
        public const string StorageFileName =  "testfile";

        public const int Iterations = 50;
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting the benchmark, {0} iterations", Config.Iterations);
            Console.WriteLine();

            Console.WriteLine("Executing benchmark with transient connection objects");
            Benchmark(new TransientFactory());

            Console.WriteLine();
            Console.WriteLine("Executing benchmark with singleton connection objects");
            Benchmark(new SingletonFactory());
        }

        private static void Benchmark(IConnectionFactory connFactory)
        {
            Console.WriteLine("Benchmarking the Datastore");
            DatastoreBenchmark(connFactory);

            Console.WriteLine();
            Console.WriteLine("Benchmarking the Cloud Storage");
            StorageBenchmark(connFactory);
        }

        private static void DatastoreBenchmark(IConnectionFactory connFactory)
        {
            var key = UpsertTestEntity(connFactory);

            var sw = Stopwatch.StartNew();

            for (int i = 0; i <= Config.Iterations; i++)
            {
                var db = connFactory.CreateDatastoreDb();

                var result = db.Lookup(key);
            }

            sw.Stop();

            Console.WriteLine("Total elapsed: {0}", sw.Elapsed);
            Console.WriteLine("Elapsed per lookup: {0}", TimeSpan.FromMilliseconds(sw.Elapsed.TotalMilliseconds / Config.Iterations));
        }

        private static Key UpsertTestEntity(IConnectionFactory connFactory)
        {
            var key = connFactory.CreateDatastoreDb().CreateKeyFactory(Config.DatastoreKind).CreateKey(Config.DatastoreKey);

            var entity = new Entity
            {
                Key = key,
                ["dummyField"] = "DummyValue"
            };

            connFactory.CreateDatastoreDb().Upsert(entity);

            return key;
        }

        private static void StorageBenchmark(IConnectionFactory connFactory)
        {
            var fileName = UploadTestFile(connFactory);

            var sw = Stopwatch.StartNew();

            for (int i = 0; i <= Config.Iterations; i++)
            {
                var client = connFactory.CreateStorageClient();

                using (var ms = new MemoryStream())
                {
                    client.DownloadObject(Config.StorageBucketName, fileName, ms);
                }
            }

            sw.Stop();

            Console.WriteLine("Total elapsed: {0}", sw.Elapsed);
            Console.WriteLine("Elapsed per lookup: {0}", TimeSpan.FromMilliseconds(sw.Elapsed.TotalMilliseconds / Config.Iterations));
        }

        private static string UploadTestFile(IConnectionFactory connFactory)
        {
            try
            {
                connFactory.CreateStorageClient().CreateBucket(Config.Project, Config.StorageBucketName);
            }
            catch
            {
            }

            using (var ms = new MemoryStream())
            {
                ms.Write(new[] { (byte)1, (byte)2, (byte)3 }, 0, 3);

                ms.Position = 0;

                connFactory.CreateStorageClient().UploadObject(
                    Config.StorageBucketName,
                    Config.StorageFileName,
                    "application/octet-stream",
                    ms);

                return Config.StorageFileName;
            }
        }
    }

    public interface IConnectionFactory
    {
        DatastoreDb CreateDatastoreDb();

        StorageClient CreateStorageClient();
    }

    public class TransientFactory : IConnectionFactory
    {
        public DatastoreDb CreateDatastoreDb() => DatastoreDb.Create(Config.Project);

        public StorageClient CreateStorageClient() => StorageClient.Create();
    }

    public class SingletonFactory : IConnectionFactory
    {
        private readonly DatastoreDb datastoreDb = DatastoreDb.Create(Config.Project);
        private readonly StorageClient storageClient = StorageClient.Create();

        public DatastoreDb CreateDatastoreDb() => datastoreDb;

        public StorageClient CreateStorageClient() => storageClient;
    }
}
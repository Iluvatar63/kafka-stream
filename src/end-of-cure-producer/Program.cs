using System;
using Coding4Fun.EndOfCure;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka;

namespace end_of_cure_producer
{
    class Program
    {
        static void Main(string[] args)
        {

            string bootstrapServers = "127.0.0.1:9092";
            string schemaRegistryUrl = "127.0.0.1:8081";
            string topicName = "end-of-cure";

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
                
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                BufferBytes = 100
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<string, EndOfCure>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<EndOfCure>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                while (true)
                {
                    var evt = new EndOfCure();
                    evt.Date = DateTime.Now.ToLongDateString();
                    evt.ItemCode = DateTime.Now.Ticks.ToString();
                    evt.CureEquipmentId = "001";

                    producer
                            .ProduceAsync(topicName, new Message<string, EndOfCure> { Key = evt.CureEquipmentId, Value = evt })
                            .ContinueWith(task =>

                                {
                                    if (!task.IsFaulted)
                                        Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                                    Console.WriteLine($"error producing message: {task.Exception.InnerException}");
                                });

                    Thread.Sleep(1000);
                }
            }
        }
    }
}

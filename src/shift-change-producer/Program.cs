using System;
using com.coding4fun.kafka.models;
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
            string topicName = "shift-changed";

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
            using (var producer = new ProducerBuilder<string, ShiftChanged>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<ShiftChanged>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                while (true)
                {
                    var evt = new ShiftChanged();
                    evt.ShiftCode = "A";
                    evt.CureEquipmentId = "001";

                    producer
                            .ProduceAsync(topicName, new Message<string, ShiftChanged> { Key = evt.CureEquipmentId, Value = evt })
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

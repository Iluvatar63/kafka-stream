using System;
using Coding4Fun.EnhancedEndOfCure;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka;

namespace enhanced_end_of_cure_consumer
{
    class Program
    {

        static void Main(string[] args)
        {

            string bootstrapServers = "127.0.0.1:9092";
            string schemaRegistryUrl = "127.0.0.1:8081";
            string topicName = "enhanced-end-of-cure";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "console-group"
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer = new ConsumerBuilder<string, EnhancedEndOfCure>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry, avroSerializerConfig).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<EnhancedEndOfCure>(schemaRegistry, avroSerializerConfig).AsSyncOverAsync())
                    .Build())
            {
                consumer.Subscribe(topicName);

                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            var v = (EnhancedEndOfCure)consumeResult.Message.Value;
                            Console.WriteLine($"End Of cure : \n\r" +
                                " - Date : " + v.Date +
                                " - ItemCode : " + v.ItemCode +
                                " - CureEquipmentId : " + v.CureEquipmentId +
                                " - ShiftCode : " + v.ShiftCode);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}
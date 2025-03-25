using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Kafka.Producer.Events;
using System.Text;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        internal async Task CreateTopicAsync(string topicName)
        {

            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"

            }).Build();

            try
            {
                var configs = new Dictionary<string, string>()
                {
                    {"message.timestamp.type","LogAppendTime" }
                };
                await adminClient.CreateTopicsAsync(new[]
                {
                 new TopicSpecification()
                 {
                      Name = topicName,
                      NumPartitions = 6,
                      ReplicationFactor = 1,
                      Configs = configs

                 }
                });
                Console.WriteLine($"Topic({topicName}) oluşturuldu!");
            }
            catch (Exception e)
            {

                Console.WriteLine("Bir Hata Oluştu");
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(e.Message);

            }
        }

        internal async Task CreateTopicWithRetentionAsync(string topicName)
        {

            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094"

            }).Build();

            try
            {
                TimeSpan day30Span = TimeSpan.FromDays(30);
                var configs = new Dictionary<string, string>()
                {
                    //{"retention.ms","-1" }//silinmez saklı kalır.
                    {"retention.ms",day30Span.TotalMilliseconds.ToString() }//30 gün saklı kalır.

                };
                await adminClient.CreateTopicsAsync(new[]
                {
                 new TopicSpecification()
                 {
                      Name = topicName,
                      NumPartitions = 6,
                      ReplicationFactor = 1,
                      Configs = configs

                 }
                });
                Console.WriteLine($"Topic({topicName}) oluşturuldu!");
            }
            catch (Exception e)
            {

                Console.WriteLine("Bir Hata Oluştu");
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(e.Message);

            }
        }

        internal async Task CreateTopicWithCluster(string topicName)
        {

            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",

            }).Build();

            try
            {
                var configs = new Dictionary<string, string>()
                {
                    {"message.timestamp.type","LogAppendTime" }
                };
                await adminClient.CreateTopicsAsync(new[]
                {
                 new TopicSpecification()
                 {
                      Name = topicName,
                      NumPartitions = 6,
                      ReplicationFactor = 3,
                      Configs = configs

                 }
                });
                Console.WriteLine($"Topic({topicName}) oluşturuldu!");
            }
            catch (Exception e)
            {

                Console.WriteLine("Bir Hata Oluştu");
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(e.Message);

            }
        }


        internal async Task SendSimpleMessageWithNullKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Message(Use Case - 1) {item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(200);
            }
        }

        internal async Task SendSimpleMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<int, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var message = new Message<int, string>()
                {
                    Value = $"Message(Use Case - 1) - {item}",
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(10);
            }
        }

        internal async Task SendComlexMessageWithIntKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config).SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).Build();

            foreach (var item in Enumerable.Range(1, 100))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 200,
                    UserId = item
                };
                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(100);
            }
        }

        internal async Task SendComlexMessageWithIntAndHeaderKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config).SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 200,
                    UserId = item
                };

                var headers = new Headers();
                headers.Add("correlation_id", Encoding.UTF8.GetBytes("1234"));
                headers.Add("version", Encoding.UTF8.GetBytes("v1"));

                var message = new Message<int, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = item,
                    Headers = headers
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(100);
            }
        }

        internal async Task SendComlexMessageWithIntAndKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config).SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).SetKeySerializer(new CustomKeySerializer<MessageKey>()).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 200,
                    UserId = item
                };


                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key value 1", "key value 2")
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(100);
            }
        }


        internal async Task SendComlexMessageWithTimeStamp(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config).SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>()).SetKeySerializer(new CustomKeySerializer<MessageKey>()).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var orderCreatedEvent = new OrderCreatedEvent()
                {
                    OrderCode = Guid.NewGuid().ToString(),
                    TotalPrice = item * 200,
                    UserId = item
                };


                var message = new Message<MessageKey, OrderCreatedEvent>()
                {
                    Value = orderCreatedEvent,
                    Key = new MessageKey("key value 1", "key value 2"),
                    //Timestamp = new Timestamp(new DateTime(2012,02,02))
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(250);
            }
        }

        internal async Task SendMessageToPartition(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj {item}"
                };
                var topicPartiton = new TopicPartition(topicName, new Partition(2));
                var result = await producer.ProduceAsync(topicPartiton, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(250);
            }
        }

        internal async Task SendMessageWithAck(string topicName)
        {
            //0-- hızlı döner ama kayıt garantisi yok
            //1 leadere kayıt yapar.
            //-1 kesin sonuç daha yavaş response.
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094",
                Acks = Acks.All
            };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj {item}"
                };
                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(250);
            }
        }

        internal async Task SendMessageWithCluster(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                Acks = Acks.All
            };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Mesaj {item}"
                };
                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }
                Console.WriteLine("----------------------------------------");
                await Task.Delay(250);
            }
        }



    }
}

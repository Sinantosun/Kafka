﻿using Confluent.Kafka;
using Kafka.Consumer.Events;
using System.Text;
using System.Xml;

namespace Kafka.Consumer
{
    internal class KafkaService
    {
        internal async Task ConsumeSimpleMessageWithNullKeyAsync(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    Console.WriteLine($"Gelen Mesaj : {consumeResult.Message.Value}");
                }
                await Task.Delay(500);
            }
        }

        internal async Task ConsumeSimpleMessageWithIntKeyAsync(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);
            int count = 0;
            while (true)
            {
                count++;
                if (count % 2 == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                }
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    Console.WriteLine($"Gelen Mesaj  Key: ${consumeResult.Message.Key}  Value : {consumeResult.Message.Value}");
                }
                await Task.Delay(250);
            }
        }

        internal async Task ConsumeComlexMessageWithIntKeyAsync(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);
            int count = 0;
            while (true)
            {
                count++;
                if (count % 2 == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                }
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    var orderCreated = consumeResult.Message.Value;

                    Console.WriteLine($"Gelen Mesaj  Key: ${consumeResult.Message.Key} OrderCode : " + orderCreated.OrderCode + "  UserId: " + orderCreated.UserId + " TotalPrice: " + orderCreated.TotalPrice);
                }
                await Task.Delay(250);
            }
        }

        internal async Task ConsumeComlexMessageWithIntKeyAndHeaderAsync(string topicName)
        {
            string message = "Veri Aranıyor";
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);
            int timer = 5;
            while (true)
            {


                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));

                    var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                    Console.WriteLine($"Headers - correlation_id : {correlationId},Version : {version}");

                    var orderCreated = consumeResult.Message.Value;

                    Console.WriteLine($"Gelen Mesaj  Key: {consumeResult.Message.Key} OrderCode : " + orderCreated.OrderCode + "  UserId: " + orderCreated.UserId + " TotalPrice: " + orderCreated.TotalPrice);
                }
                else
                {
                    Console.WriteLine(message);
                    for (int i = 1; i <= 5; i++)
                    {
                        Console.Write(i);
                        await Task.Delay(1000);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Veri Bulunamadı");
                }
                await Task.Delay(100);
            }
        }

        internal async Task ConsumeComlexMessageKeyAndAsync(string topicName)
        {
            string message = "Veri Aranıyor";
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>()).Build();
            consumer.Subscribe(topicName);
            while (true)
            {



                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Key;
                    var orderCreated = consumeResult.Message.Value;
                    Console.WriteLine($"Gelen Mesaj  Key1 : {messageKey.Key1} Key2 : {messageKey.Key2} OrderCode : " + orderCreated.OrderCode + "  UserId: " + orderCreated.UserId + " TotalPrice: " + orderCreated.TotalPrice);
                }
                else
                {
                    Console.WriteLine(message);
                    for (int i = 1; i <= 5; i++)
                    {
                        Console.Write(i);
                        await Task.Delay(1000);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Veri Bulunamadı");
                }
            }
        }

        internal async Task ConsumeComlexMessageWithTimeStamp(string topicName)
        {
            string message = "Veri Aranıyor";
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).SetKeyDeserializer(new CustomKeyDeserializer<MessageKey>()).Build();
            consumer.Subscribe(topicName);
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message TimeStamp : {consumeResult.Message.Timestamp.UtcDateTime}");

                }
                await Task.Delay(100);
            }
        }

        internal async Task ConsumeToPartition(string topicName)
        {
            //beliril partiotion dan okuma.
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Assign(new TopicPartition(topicName, 2));
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message TimeStamp : {consumeResult.Message}");

                }
                await Task.Delay(100);
            }
        }

        internal async Task ConsumeToPartitionFromOffset(string topicName)
        {
            //beliril partiotion dan okuma.
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-3",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Assign(new TopicPartitionOffset(topicName, 2, 4));
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message TimeStamp : {consumeResult.Message.Value}");

                }
                await Task.Delay(100);
            }
        }

        internal async Task ConsumMessageWithAck(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-2-group-3",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message TimeStamp : {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);

                }
                await Task.Delay(100);
            }
        }

        internal async Task ConsumMessageFromCluster(string topicName)
        {

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
                GroupId = "group-x",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
            var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);
            while (true)
            {
                var consumeResult = consumer.Consume(5000);

                if (consumeResult != null)
                {
                    Console.WriteLine($"Message TimeStamp : {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);

                }
                await Task.Delay(100);
            }
        }
    }
}

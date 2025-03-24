using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "use-case-3-topic";

var kafkaService = new KafkaService();
await kafkaService.ConsumeComlexMessageWithIntKeyAndHeaderAsync(topicName);
Console.ReadLine();
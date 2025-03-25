using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "use-case-4-topic";

var kafkaService = new KafkaService();
await kafkaService.ConsumeToPartitionFromOffset(topicName);
Console.ReadLine();
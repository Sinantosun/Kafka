using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
var topicName = "my-cluster2-topic";

var kafkaService = new KafkaService();
await kafkaService.ConsumMessageFromCluster(topicName);
Console.ReadLine();
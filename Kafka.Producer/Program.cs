

using Kafka.Producer;

Console.WriteLine("Kafka, Producer! Topic Oluşturuyor!");
Console.WriteLine("----------------------------------");
var topicName = "my-cluster2-topic";

var kafkaService = new KafkaService();

await kafkaService.CreateTopicWithCluster(topicName);
Console.WriteLine("----------------------------------");
Console.WriteLine();
await kafkaService.SendMessageWithCluster(topicName);

Console.WriteLine("Mesajlar Gönderilmiştir!");


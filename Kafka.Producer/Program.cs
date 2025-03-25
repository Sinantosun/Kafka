

using Kafka.Producer;

Console.WriteLine("Kafka, Producer! Topic Oluşturuyor!");
Console.WriteLine("----------------------------------");
var topicName = "use-case-4-topic";

var kafkaService = new KafkaService();

await kafkaService.CreateTopicAsync(topicName);
Console.WriteLine("----------------------------------");
Console.WriteLine();
await kafkaService.SendMessageToPartition(topicName);

Console.WriteLine("Mesajlar Gönderilmiştir!");


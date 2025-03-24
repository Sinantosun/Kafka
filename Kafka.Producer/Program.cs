

using Kafka.Producer;

Console.WriteLine("Kafka, Producer! Topic Oluşturuyor!");
Console.WriteLine("----------------------------------");
var topicName = "use-case-3-topic";

var kafkaService = new KafkaService();

await kafkaService.CreateTopicAsync(topicName);
Console.WriteLine("----------------------------------");
Console.WriteLine();
await kafkaService.SendComlexMessageWithIntAndHeaderKey(topicName);

Console.WriteLine("Mesajlar Gönderilmiştir!");


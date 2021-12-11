package kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

    @Value(value = "${kafka.topic}")
    private String topic;

    @Value(value = "${kafka.greetings}")
    private String greetings;

    @Scheduled(fixedRate = 1000)
    void test() {
        sendMessage("test message " + System.currentTimeMillis());
        sendMessage("Hello World" + System.currentTimeMillis());
        sendGreeting(new Greeting("Greetings", "World " + System.currentTimeMillis()));
    }

    public void sendMessage(String msg) {
        kafkaTemplate.send(topic, msg);
    }

    public void sendGreeting(Greeting greeting) {
        greetingKafkaTemplate.send(greetings, greeting);
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = KafkaConsumerConfig.GROUP)
    public void listenGroupFoo(String message) {
        log.info("Received Message in group foo: " + message);
    }

    @KafkaListener(topics = "${kafka.topic}")
    public void listenerWithHeaders(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Received message: " + message + " from partition: " + partition);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topic}", partitionOffsets = {
            @PartitionOffset(partition = "0", initialOffset = "0"),
            @PartitionOffset(partition = "3", initialOffset = "0")
    }), containerFactory = "partitionsKafkaContainerFactory")
    public void listenToPartition(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("Received partition message: " + message + " from partition: " + partition);
    }

    @KafkaListener(topics = "${kafka.topic}",
            containerFactory = "filterKafkaContainerFactory")
    public void listenWithFilter(String message) {
        log.info("Received filtered message: " + message);
    }

    @KafkaListener(topics = "${kafka.greetings}", containerFactory = "greetingsKafkaContainerFactory")
    public void greetingsListener(Greeting greeting) {
        log.info("Greeting received: " + greeting.getMsg() + " " + greeting.getName());
    }

}

package kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class MessageService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${kafka.topic}")
    private String topic;

    @Scheduled(fixedRate = 1000)
    void test() {
        sendMessage("test message");
    }

    public void sendMessage(String msg) {
        kafkaTemplate.send(topic, msg);
    }

    @KafkaListener(topics = "mytopic", groupId = KafkaConsumerConfig.GROUP)
    public void listenGroupFoo(String message) {
        System.out.println("Received Message in group foo: " + message);
    }

}

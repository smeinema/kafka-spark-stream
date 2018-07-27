package eu.luminis.amsterdam.kafkasparkstream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Development controller to test things.
 */
@Profile("dev")
@RequestMapping("/dev")
@RestController
public class DevController {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    public DevController(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/produce/twitter")
    public void produceMessageOnTopicTwitter(@RequestBody String message) {
        produceMessageOnTopic("twitter", message);
    }

    @PostMapping("/produce/test")
    public void produceMessageOnTopicTest(@RequestBody String message) {
        produceMessageOnTopic("test", message);
    }

    private void produceMessageOnTopic(String topic, String message) {
        System.out.println("Producing message on topic \"" + topic + "\": " + message);
        kafkaTemplate.send(topic, message);
        kafkaTemplate.flush();
    }

}

package eu.luminis.amsterdam.kafkasparkstream;

import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

public class CustomKafkaListener {

    private final CountDownLatch latch1 = new CountDownLatch(1);

    @KafkaListener(id = "customKafkaListener", topics = {"test", "twitter"})
    public void listen1(String message) {
        System.out.println("customKafkaListener consumed: " + message);
        this.latch1.countDown();
    }

}

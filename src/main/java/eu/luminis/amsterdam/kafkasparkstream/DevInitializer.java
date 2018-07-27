package eu.luminis.amsterdam.kafkasparkstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

//import org.apache.spark.streaming.kafka.*;

@Profile("dev")
@Component
public class DevInitializer {

    private static final String TEST_TOPIC = "test";

    @Value("${kafka.address}")
    private String kafkaAddress;

    @Value("#{systemProperties['kafka.topic'] ?: 'test'}")
    private String kafkaTopic;

    @Autowired
    private ProducerFactory<Integer, String> producerFactory;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Resource(name = "consumerConfigs")
    private Map<String, Object> consumerConfigs;

    @Value("${spark.address}")
    private String sparkAddress;

//    @Autowired
    private JavaStreamingContextService javaStreamingContextService;

    @Bean
    CommandLineRunner testDevInitializer() {
        return args -> {
            System.out.println("DevInitializer is running!");
            System.out.println("kafkaAddress = " + kafkaAddress);
            System.out.println("kafkaTopic = " + kafkaTopic);
            System.out.println("sparkAddress = " + sparkAddress);
        };
    }

    @Bean
    CommandLineRunner testProduceAndConsumeMessage() {
        return args -> {
            System.out.println("Start auto test produce and consume message");
            ContainerProperties containerProps = new ContainerProperties(TEST_TOPIC);
            final CountDownLatch latch = new CountDownLatch(4);
            containerProps.setMessageListener(new MessageListener<Integer, String>() {

                @Override
                public void onMessage(ConsumerRecord<Integer, String> message) {
                    System.out.println("received: " + message);
                    latch.countDown();
                }

            });
            KafkaMessageListenerContainer<Integer, String> container = createContainer(containerProps);
            container.setBeanName("testAutoProduceAndConsumeBean");
            container.start();
            Thread.sleep(1000); // wait a bit for the container to start

            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            kafkaTemplate.setDefaultTopic(TEST_TOPIC);
            kafkaTemplate.sendDefault(0, "foo");
            kafkaTemplate.sendDefault(2, "bar");
            kafkaTemplate.sendDefault(0, "baz");
            kafkaTemplate.sendDefault(2, "qux");
            kafkaTemplate.flush();

            container.stop();
            System.out.println("Stop auto test produce and consume message");
        };
    }

    private KafkaMessageListenerContainer<Integer, String> createContainer(ContainerProperties containerProps) {
        Map<String, Object> props = new HashMap<>(consumerConfigs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-auto-consumer");
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
        return new KafkaMessageListenerContainer<>(cf, containerProps);
    }



//    @Bean
    CommandLineRunner createKafkaAndSparkConnections() {
        return args -> {
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", kafkaAddress);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "spark-streaming-context-consumer");
            kafkaParams.put("auto.offset.reset", "earliest");
            kafkaParams.put("enable.auto.commit", false);

            Collection<String> topics = Arrays.asList(kafkaTopic, TEST_TOPIC);

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            javaStreamingContextService.getJavaStreamingContext(),
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

            stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        };
    }

//    @Bean
    CommandLineRunner testKafkaAndSparkConnections() {
        return args -> {
            System.out.println("start testKafkaAndSparkConnections");
            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            kafkaTemplate.setDefaultTopic(kafkaTopic);
            kafkaTemplate.sendDefault(0, "tweet 1");
            kafkaTemplate.sendDefault(1, "tweet 2");
            kafkaTemplate.sendDefault(2, "tweet 3");
            kafkaTemplate.sendDefault(3, "tweet 4");
            kafkaTemplate.flush();
            System.out.println("stop testKafkaAndSparkConnections");
        };
    }

}

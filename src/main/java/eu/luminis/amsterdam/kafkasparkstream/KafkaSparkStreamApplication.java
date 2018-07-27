package eu.luminis.amsterdam.kafkasparkstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@ComponentScan({})
@Import({})
@PropertySource({
        "classpath:default.properties",
        "classpath:environment.properties"
        // The second properties file overrides properties of the first.
})
public class KafkaSparkStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSparkStreamApplication.class, args);
    }
}

package eu.luminis.amsterdam.kafkasparkstream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

//@Service
public class JavaStreamingContextService {

    private final JavaStreamingContext javaStreamingContext;

    public JavaStreamingContextService(@Value("${spark.address}") String sparkClusterAddress) {
        // Create a StreamingContext with a batch interval of 1 second
        String master = "spark://" + sparkClusterAddress;
        SparkConf conf = new SparkConf().setMaster(master).setAppName("KafkaClusterSparkStreamingConnector");
        javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
    }

    public JavaStreamingContext getJavaStreamingContext() {
        return javaStreamingContext;
    }
}

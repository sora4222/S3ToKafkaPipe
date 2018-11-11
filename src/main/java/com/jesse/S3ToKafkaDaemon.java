package com.jesse;

import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.idempotent.kafka.KafkaIdempotentRepository;

public class S3ToKafkaDaemon {
    private final CamelContext camelContext;

    public S3ToKafkaDaemon(String s3url, String kafka_host, String kafka_topic) throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure(){
                KafkaIdempotentRepository repository = new KafkaIdempotentRepository(
                        "camel_idempotent_repository",
                        kafka_host + ":9092");
                from("aws-s3:" + s3url +
                        "?useIAMCredentials=True" +
                        "&deleteAfterRead=False" +
                        "&region=AP_SOUTHEAST_2")
                        .idempotentConsumer(header("CamelAwsS3Key"), repository)
                        .log(
                                LoggingLevel.INFO,
                                "${header.CamelAwsS3Key} is being downloaded and has passed " +
                                        "the idempotentConsumer")
                        .split(bodyAs(String.class).tokenize("\n"))
                        .to("kafka:" + kafka_topic + "?brokers=" + kafka_host + ":9092")
                        .end();
            }
        });
    }

    public void start() throws Exception {
        camelContext.start();
    }

    public void stop() throws Exception {
        camelContext.stop();
    }
}

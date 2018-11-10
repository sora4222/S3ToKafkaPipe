package com.jesse;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class S3ToKafkaDaemon {
    private final CamelContext camelContext;
    public S3ToKafkaDaemon(String s3url, String kafka_host, String kafka_topic) throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("aws-" + s3url + "?useIAMCredentials=True&deleteAfterRead=False")
                        .split(bodyAs(String.class).tokenize("\n"))
                        .to("kafka:"+ kafka_topic + "?brokers=" + kafka_host + ":9092");
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

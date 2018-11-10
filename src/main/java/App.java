import com.jesse.S3ToKafkaDaemon;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {
    private static Properties LoadProperties() throws IOException{
        Properties kafkaToS3Properties = new Properties();
        InputStream kafkaToS3FileStream = App.class.getResourceAsStream("S3ToKafka.properties");
        kafkaToS3Properties.load(kafkaToS3FileStream);
        return kafkaToS3Properties;
    }

    public static void main(String[] args) {
        Properties kafkaToS3Properties = null;
        try {
            kafkaToS3Properties = LoadProperties();
        }
        catch (IOException exc){
            System.out.print(exc.getMessage());
            System.out.print("The properties file doesn't appear valid, shutting down");
        }


        try {
            S3ToKafkaDaemon kafkaDaemon = new S3ToKafkaDaemon(kafkaToS3Properties.getProperty("s3url"),
                    kafkaToS3Properties.getProperty("kafka_host"),
                    kafkaToS3Properties.getProperty("kafka_topic"));

            kafkaDaemon.start();
            Thread.sleep(100000);

        }
        catch (Exception exc){
            System.out.print("Camel has thrown an exception\n");
            System.out.print(exc.getMessage());
        }
	System.out.print("Everything went ok!");

    }
}

import com.jesse.S3ToKafkaDaemon;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.System.exit;

public class App {
    private static Properties LoadProperties() throws IOException{
        Properties kafkaToS3Properties = new Properties();
        InputStream kafkaToS3FileStream = InputStream.class.getResourceAsStream("S3ToKafka.properties");
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
            exit(1);
        }

        try {
            S3ToKafkaDaemon kafkaDaemon = new S3ToKafkaDaemon(kafkaToS3Properties.getProperty("s3url"),
                    kafkaToS3Properties.getProperty("kafka_host"),
                    kafkaToS3Properties.getProperty("kafka_topic"));
        }
        catch (Exception exc){
            System.out.print("Camel has thrown an exception");
            System.out.print(exc.getMessage());
        }
    }
}

package kudadiri.DataEngineer.portofolio;

import kudadiri.DataEngineer.portofolio.config.KafkaConfig;
import kudadiri.DataEngineer.portofolio.utility.CsvReaderService;
import kudadiri.DataEngineer.portofolio.utility.KafkaProducerService;

import kudadiri.kafka.serializer.DataRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.List;

public class KafkaProducerApp {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApp.class.getSimpleName());
    private static final Logger logAnalytics = LoggerFactory.getLogger("AnalyticsLogs");

    public static void main(String[] args) {

//        if (args.length == 1 && "--version".equals(args[0])) {
//            System.out.println("Kafka Producer App v1.0.0");
//            System.out.println("Built with Kafka 3.8.0");
//            return;
//        } else if (args.length != 5) {
//            System.out.println("[IMPORTANT] Need Configuration Variables <ProducerId, bootstrap-server, topic, datasetPath, input-rate-per-second>...");
//            return;
//        } else {
//            logger.info("Access Granted, Program Running Producer With Name {}", args[0]);
//        }
//
//        String producerId = args[0];
//        String servers = args[1];
//        String topic = args[2];
//        String filePath = args[3];
//        int inputRate = Integer.parseInt(args[4]) / 2;
        String producerId = "producer-1";
        String servers = "172.25.5.7:9092";
//        String topic = "records-example";
        String topic = "topic-network";
        int inputRate = 200;
        String filePath = "Dataset/skenario2.csv";

        KafkaProducerService producer = new KafkaProducerService(
                KafkaConfig.getProducerConfig(servers, producerId),
                topic,
                logger,
                logAnalytics,
                producerId
        );

        List<DataRecord> records = CsvReaderService.readCsv(filePath);
        producer.sendData(records, inputRate);

        producer.close();
    }
}
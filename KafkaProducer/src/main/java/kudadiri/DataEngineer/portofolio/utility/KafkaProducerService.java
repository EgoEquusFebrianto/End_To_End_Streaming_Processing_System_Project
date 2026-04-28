package kudadiri.DataEngineer.portofolio.utility;

import kudadiri.kafka.serializer.DataRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import java.util.List;
import java.util.Properties;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class KafkaProducerService {
    private KafkaProducer<String, DataRecord> producer;
    private String topic;
    private Logger logger;
    private Logger logAnalytics;
    private String producerId;

    public KafkaProducerService(Properties props, String topic, Logger logger, Logger logAnalytics, String producerId) {
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.logger = logger;
        this.logAnalytics = logAnalytics;
        this.producerId = producerId;
    }

    public void sendData(List<DataRecord> records, int ratePerSecond) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        try {
            long interval = 1_000_000_000L / ratePerSecond; // nanoseconds
            long nextSendTime = System.nanoTime();

            for (DataRecord record : records) {
                long now;

                // menunggu sampai waktunya kirim
                while ((now = System.nanoTime()) < nextSendTime) {
                    if ( nextSendTime - now > 1_000_000) {
                        Thread.sleep(0, 500_000);
                    }
                }

//                // Mekanisme menambahkan timestamp untuk evaluasi latency
//                final long producerTimestamp = record.getProducerTimestamp();
//                final String forwardedStr = record.getProducerTimestampStr().toString();

                // Mekanisme menambahkan timestamp untuk evaluasi latency
                final long producerTimestamp = System.currentTimeMillis();
                record.setProducerTimestamp(producerTimestamp);

                // Convert ke datetime
                LocalDateTime dataForwardedTime = Instant.ofEpochMilli(producerTimestamp)
                        .atZone(ZoneId.of("UTC"))
                        .toLocalDateTime();

                String forwardedStr = dataForwardedTime.format(formatter);
                record.setProducerTimestampStr(forwardedStr);

                ProducerRecord<String, DataRecord> kafkaRecord = new ProducerRecord<>(topic, record);

                producer.send(kafkaRecord, (metadata, e) -> {
                    if (e == null) {
                        long brokerTimestamp = metadata.timestamp();
                        long latency = brokerTimestamp - producerTimestamp;

                        LocalDateTime dataReceiveTime = Instant.ofEpochMilli(brokerTimestamp)
                                .atZone(ZoneId.of("UTC"))
                                .toLocalDateTime();

                        String receivedStr = dataReceiveTime.format(formatter);

                        System.out.printf("Data Successfully send with Flow ID: %s%n", record.getFlowID());
                        logAnalytics.info(
                                "[{}] Data Sent with Flow ID: {}, dataForwardedTime:{}, dataReceiveTime:{}, brokerTimeStamp:{}, latency={}",
                                producerId,
                                record.getFlowID(),
                                forwardedStr,
                                receivedStr,
                                brokerTimestamp,
                                latency
                        );
                    } else {
                        logger.error("[{}] Failed to send data to Kafka. Topic: {}", producerId, topic, e);
                    }
                });

                // Jadwalkan waktu kirim berikutnya
                nextSendTime += interval;

            }

        } catch (Exception e) {
            logger.error(
                    "[{}] Error while preparing or sending data to Kafka. Topic: {}",
                    producerId, topic, e
            );
        }
    }

    public void close() {
        producer.close();
        logger.info("[{}] KafkaApp Terminate...", producerId);
    }
}

package com.skyfree.storm.ml.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.util.Properties;
import java.util.Scanner;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 13:43
 */
public class KafkaProducer {
    public static void main(String[] args) throws IOException {
        // 主要是将训练数据80% 导入到kafka中, 20%留作测试数据
        System.out.println("finished");
    }

    private static void init() throws IOException {
        Properties props = new Properties();

        props.put("metadata.broker.list", "skyfree3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        File file = new File("dataset/data");
        System.out.println(file.getAbsolutePath());
        Scanner scanner = new Scanner(file);

        File preditToFile = new File("dataset/prediction.data");

        BufferedWriter writer = new BufferedWriter(new FileWriter(preditToFile));

        int i = 0;

        while (scanner.hasNextLine()) {
            String instance = scanner.nextLine();

            if (i++ % 5 == 0) {
                writer.write(instance + "\n");
            } else {
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("training", instance);
                producer.send(data);
            }
        }

        scanner.close();
        writer.close();
        producer.close();

        System.out.println("Produced data");
    }
}

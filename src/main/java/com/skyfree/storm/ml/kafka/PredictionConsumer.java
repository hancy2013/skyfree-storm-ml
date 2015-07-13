package com.skyfree.storm.ml.kafka;

import backtype.storm.LocalDRPC;
import scala.reflect.generic.UnPickler;

import java.io.*;
import java.util.Scanner;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 14:30
 */
public class PredictionConsumer {
    private final LocalDRPC drpc;

    private final String input;

    private final String output;

    public PredictionConsumer(LocalDRPC drpc, String input, String output) {
        this.drpc = drpc;
        this.input = input;
        this.output = output;
    }

    public void predict() throws IOException {
        Scanner scanner = new Scanner(new File(this.input));

        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output)));

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            if (line.trim().length() == 1) {
                continue;
            }

            String prediction = drpc.execute("predict", line);
            writer.write(prediction + "\n");
        }

        scanner.close();
        writer.close();
    }
}

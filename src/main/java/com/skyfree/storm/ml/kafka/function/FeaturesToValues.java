package com.skyfree.storm.ml.kafka.function;

import backtype.storm.tuple.Values;
import com.github.pmerienne.trident.ml.core.Instance;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 14:13
 */
public class FeaturesToValues extends BaseFunction {
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String line = tridentTuple.getString(0);

        double[] features = new double[60];

        String[] featureList = line.split("\\s+");

        for (int i = 0; i < features.length; i++) {
            features[i] = Double.parseDouble(featureList[i]);
        }

        tridentCollector.emit(new Values(new Instance(features)));
    }
}

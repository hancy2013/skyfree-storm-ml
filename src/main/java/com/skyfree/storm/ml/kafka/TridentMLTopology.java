package com.skyfree.storm.ml.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.pmerienne.trident.ml.clustering.ClusterQuery;
import com.github.pmerienne.trident.ml.clustering.ClusterUpdater;
import com.github.pmerienne.trident.ml.clustering.KMeans;
import com.skyfree.storm.ml.kafka.function.FeaturesToValues;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;

import java.io.IOException;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/13 14:19
 */
public class TridentMLTopology {
    public static void main(String[] args) throws InterruptedException, IOException {
        local();
    }

    public static void local() throws InterruptedException, IOException {
        BrokerHosts brokerHosts = new ZkHosts("skyree1:2181");

        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "training", "trident-ml-client");

        kafkaConfig.forceFromStart = true;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);

        TridentTopology topology = new TridentTopology();

        TridentState kmeansState = topology.newStream("samples", kafkaSpout)
                .each(new Fields("str"), new FeaturesToValues(), new Fields("instance"))
                .partitionPersist(new MemoryMapState.Factory(), new Fields("instance"), new ClusterUpdater("kmeans", new KMeans(6)));

        LocalDRPC localDRPC = new LocalDRPC();

        topology.newDRPCStream("predict", localDRPC)
                .each(new Fields("args"), new FeaturesToValues(), new Fields("instance"))
                .stateQuery(kmeansState, new Fields("instance"), new ClusterQuery("kmeans"), new Fields("prediction"));

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("kmeans", new Config(), topology.build());

        Thread.sleep(90000);

        PredictionConsumer predictionConsumer = new PredictionConsumer(localDRPC, "dataset/prediction.data", "dataset/predicted.data");

        predictionConsumer.predict();

        cluster.shutdown();
        localDRPC.shutdown();
    }
}

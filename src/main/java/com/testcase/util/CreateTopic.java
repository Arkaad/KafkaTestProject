package com.testcase.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 20-Feb-18.
 */
public class CreateTopic {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("TextLinesTopic", "RekeyedIntermediateTopic", "WordsWithCountsTopic"));
        KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();
        Map<String, TopicDescription> stringTopicDescriptionMap = all.get();
        for (Map.Entry<String, TopicDescription> descriptionEntry : stringTopicDescriptionMap.entrySet()) {
            System.out.println("Key - >" + descriptionEntry.getKey() + " Value ->" + descriptionEntry.getValue().name());
        }
    }
}

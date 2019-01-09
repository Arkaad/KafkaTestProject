package com.testcase.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by Arka Dutta on 20-Feb-18.
 */
public class CreateTopic {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AdminClient adminClient = null;
        try {
            Properties props = new Properties();
            props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Utility.BOOTSTRAP_SERVERS);
            adminClient = AdminClient.create(props);

            String topicName = "Words-Counts-Topic";
            NewTopic newTopic = new NewTopic(topicName, 3, (short)1);
            CreateTopicsResult topics = adminClient.createTopics(Collections.singleton(newTopic));
            topics.values().get(topicName).get();
            System.out.println(topics.toString() + " created successfully.");
        } catch (TopicExistsException ex) {
            ex.printStackTrace();
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }

    }
}

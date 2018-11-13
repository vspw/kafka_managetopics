package com.hwx.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.SecurityProtocol;

/*
 * Can be used to create Kafka Topics using the AdminClient API
 * part of Kafka 0.11.x.
 * Update the jaas file to add the correct keytab and username
 * 
 */

public class KafkaCreateTopicAdminClient {

  public static void main(String[] args) {
	  
		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\jaas.conf");
			
		}
		
    String topic = "hwxTestAdminClient1";
    Properties topicConfig = new Properties(); // add per-topic configurations settings here
    
    topicConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-30-147-33.saccap.int:9100");
    topicConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
    

    AdminClient admin = AdminClient.create(topicConfig);

    Map<String, String> configs = new HashMap<String, String>();
    int partitions = 1;
    int replication = 1;
    List<NewTopic>  topicList = new ArrayList<NewTopic>();
    topicList.add(new NewTopic(topic, partitions, (short) replication).configs(configs));
    CreateTopicsResult result =admin.createTopics(topicList);
    KafkaFuture<Void> all = result.all();
    try {
		all.get();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    System.out.println(all.isDone());
    admin.close();

  }

}
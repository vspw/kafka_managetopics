package com.hwx.kafka;



import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.SecurityProtocol;

public class KafkaDeleteTopicAdminClient {

  public static void main(String[] args) {
	  
		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\jaas.conf");
			
		}
		
    String topic = "hwxTestAdminClient1";
    Properties topicConfig = new Properties(); // add per-topic configurations settings here
    
    topicConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hwx.kafka.node.com:9100");
    topicConfig.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
    

    AdminClient admin = AdminClient.create(topicConfig);

    List<String>  topicList = new ArrayList<String>();
    topicList.add(topic);
    DeleteTopicsResult result =admin.deleteTopics(topicList);
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
/*
 * Can be used to create Kafka Topics using the Zookeeper Utils API
 * Update the jaas file to add the correct keytab and username
 * 
 */

package com.hwx.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaCreateTopicZKClient {

  public static void main(String[] args) {
	  //Update the path of the jaas.conf
		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\jaas.conf");
			
		}
		
    String zookeeperConnect = "zknode.hwx.com:2181";

    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;

    String topic = "topic_temp5";
    int partitions = 20;
    int replication = 3;
    Properties topicConfig = new Properties(); // add per-topic configurations settings here

    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(
        zookeeperConnect,
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);

    // Security for Kafka was added in Kafka 0.9.0.0
    boolean isSecureKafkaCluster = true;

    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
    zkClient.close();
  }

}
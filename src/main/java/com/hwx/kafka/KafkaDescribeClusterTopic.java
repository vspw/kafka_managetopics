package com.hwx.kafka;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.protocol.SecurityProtocol;


public class KafkaDescribeClusterTopic {
	static int MAX_RETRY_COUNT=10;
	static Properties topicProperties;

	public static void main(String[] args) {

		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\Users\\VenkataW\\eclipse-workspace\\managetopics\\resources\\jaas.conf");

		}
		//"hwxTestAdminClient1", "firm-ref-currency-byCode_temp","firm-monitor-topic"
		String[] topics = {"__consumer_offsets"};
		topicProperties = new Properties();

		topicProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.hwx.com:9100");
		topicProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
		AdminClient admin = AdminClient.create(topicProperties);

		//Describe the Cluster
		describeKafkaCluster(admin);
		//Describe each kafka topics in the list
		describeKafkaTopics(admin, topics);
		//Try to consume few records from each kafka topic in the list
		//consumeSampleFromTopicList(admin, topics);
		consumeAllMessagesForCount(admin,topics);
		admin.close();

	}
	protected static void describeKafkaCluster(AdminClient admin)
	{
		DescribeClusterResult cluster = admin.describeCluster();
		KafkaFuture<String> all = cluster.clusterId();
		KafkaFuture<Collection<Node>> allNodes = cluster.nodes();
		KafkaFuture<Node> controllerNode = cluster.controller();

		try {
			String clusterId = all.get();
			System.out.println("####################ClusterID: ###################: " +clusterId);
			Collection<Node> nodeList = new ArrayList<Node>();
			nodeList=allNodes.get();
			Iterator<Node> nodeIterator = nodeList.iterator();
			while (nodeIterator.hasNext()) {
				Node n= nodeIterator.next();
				System.out.println(n.host()+ " | id: " +n.id());	
			}
			Node controller = controllerNode.get();
			System.out.println("Controller Node: "+controller.host());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected static void describeKafkaTopics(AdminClient admin, String[] topics )
	{
		Collection<String> listTopics = Arrays.asList(topics);

		DescribeTopicsResult descTopic = admin.describeTopics(listTopics);
		Map<String, TopicDescription> topicDetails = new HashMap<String, TopicDescription>();
		KafkaFuture<Map<String, TopicDescription>> topicDetailsFuture = descTopic.all();
		try {
			topicDetails= topicDetailsFuture.get();
			//System.out.println(new PrettyPrintingMap<String, TopicDescription>(topicDetails));
			Iterator<Entry<String, TopicDescription>> iteratorDesc=topicDetails.entrySet().iterator();
			//Iterate over each topic in the list
			while(iteratorDesc.hasNext())
			{

				Entry<String, TopicDescription> topicDescript =  iteratorDesc.next();
				System.out.println("##########################Topic Key:################################:  " +topicDescript.getKey());

				TopicDescription tpdesc = topicDescript.getValue();
				List<TopicPartitionInfo> partInfoList = tpdesc.partitions();
				Iterator<TopicPartitionInfo> partInfoListIterator = partInfoList.iterator();
				while(partInfoListIterator.hasNext())
				{
					TopicPartitionInfo partInfo = partInfoListIterator.next();
					System.out.println("Leader information: Leader: "+partInfo.leader().host() + " ID:  " + partInfo.leader().id());
					System.out.println("Replica information: "+partInfo.replicas().toString()+ " ISRs: "+ partInfo.isr().toString());
				}
			}
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected static void consumeSampleFromTopicList(AdminClient admin, String[] topics)
	{
		Collection<String> listTopics = Arrays.asList(topics);

		DescribeTopicsResult descTopic = admin.describeTopics(listTopics);
		Map<String, TopicDescription> topicDetails = new HashMap<String, TopicDescription>();
		KafkaFuture<Map<String, TopicDescription>> topicDetailsFuture = descTopic.all();
		try {
			topicDetails= topicDetailsFuture.get();
			//System.out.println(new PrettyPrintingMap<String, TopicDescription>(topicDetails));
			Iterator<Entry<String, TopicDescription>> iteratorDesc=topicDetails.entrySet().iterator();
			//Iterate over each topic in the list
			while(iteratorDesc.hasNext())
			{

				Entry<String, TopicDescription> topicDescript =  iteratorDesc.next();
				System.out.println("##########################Topic Key:################################:  " +topicDescript.getKey());

				TopicDescription tpdesc = topicDescript.getValue();

				/*
				 * Consume from each topic
				 * 
				 */
				topicProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "describeTopic1"+tpdesc.name());
				topicProperties.put("enable.auto.commit", "false");
				topicProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
				topicProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
				topicProperties.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());

				System.out.println("--- initializing consumer---");
				KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(topicProperties);

				//consumer.subscribe(Collections.singletonList(tpdesc.name()));
				consumer.subscribe(Arrays.asList(tpdesc.name()));
				consumer.poll(10);
				consumer.seek(new TopicPartition(tpdesc.name(),0),0);
				int exitCounter=0;
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					System.out.println("records size " + records.count());

					for (ConsumerRecord<String, String> record : records) {
						System.out.println("Received message key: " + record.key());
						System.out.println("Message Value: " +record.value());
						System.out.println("Message offset: " +record.offset());
						System.out.println("Message partition: " +record.partition());
					}

					exitCounter++;
					if (records.count()>0||exitCounter>=MAX_RETRY_COUNT)
					{
						consumer.close();
						break;
					}
				}
				//End of Consume from Each topic
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected static void consumeAllMessagesForCount(AdminClient admin, String[] topics)
	{
		Collection<String> listTopics = Arrays.asList(topics);

		DescribeTopicsResult descTopic = admin.describeTopics(listTopics);
		Map<String, TopicDescription> topicDetails = new HashMap<String, TopicDescription>();
		KafkaFuture<Map<String, TopicDescription>> topicDetailsFuture = descTopic.all();
		KafkaConsumer<String, String> consumer=null;
		try {
			topicDetails= topicDetailsFuture.get();
			//System.out.println(new PrettyPrintingMap<String, TopicDescription>(topicDetails));
			Iterator<Entry<String, TopicDescription>> iteratorDesc=topicDetails.entrySet().iterator();
			//Iterate over each topic in the list
			while(iteratorDesc.hasNext())
			{

				Entry<String, TopicDescription> topicDescript =  iteratorDesc.next();
				System.out.println("##########################Topic Key:################################:  " +topicDescript.getKey());

				TopicDescription tpdesc = topicDescript.getValue();

				/*
				 * Consume from each topic
				 * 
				 */
				topicProperties.put("group.id", "ManageTopics3-"+tpdesc.name());
				topicProperties.put("enable.auto.commit", "true");
				topicProperties.put("auto.commit.interval.ms", "1000");
				topicProperties.put("session.timeout.ms", "30000");
				topicProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				topicProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				consumer = new KafkaConsumer<String,String>(topicProperties);
				// Either Subscribe to a topic
				//consumer.subscribe(Arrays.asList(tpdesc.name()));
				//OR assign a partition to the consumer and seek to the beginning of the partition.
				consumer.assign(Arrays.asList(new TopicPartition(tpdesc.name(), 0)));
				System.out.println(consumer.beginningOffsets(Arrays.asList(new TopicPartition(tpdesc.name(), 0))));
				consumer.seekToBeginning(Arrays.asList(new TopicPartition(tpdesc.name(), 0)));
				
				int countMessages=0;
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(100);
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("partition =  %s, offset = %d, key = %s , value = %s", record.partition(), record.offset(), record.key(), record.value());
						System.out.println("position: "+consumer.position(new TopicPartition(tpdesc.name(), record.partition())));
						countMessages++;
						System.out.println("MessageCount:"+ countMessages);
					}
					
				}
				
			}
		}
		catch(Exception e)
		{
			
		e.printStackTrace();
		}
		finally
		{
			consumer.close();
		
		}
	}

		}

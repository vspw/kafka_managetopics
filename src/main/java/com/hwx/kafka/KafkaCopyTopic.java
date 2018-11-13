package com.hwx.kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Bytes;
/*
 * This code can be used to copy data from topic[0] to topic[1]
 * The intent is to preserve the mapping between partition and data
 * 
 */

public class KafkaCopyTopic {
	static int MAX_RETRY_COUNT=10;
	static Properties consumerProperties;
	static Properties producerProperties;
	public static void main(String[] args) {

		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\Users\\VenkataW\\eclipse-workspace\\managetopics\\resources\\jaas.conf");

		}
		//array of only two topics, first one is the source topic and second one is the destination topic
		String[] topics = {"transaction1","transaction-15Million"};
		consumerProperties = new Properties();
		consumerProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hwx.kafka.com:9100");
		consumerProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
		consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
		
		producerProperties = new Properties();
		producerProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hwx.kafka.com:9100");
		producerProperties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.toString());
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.BytesSerializer");
		System.out.println("debug1 before admin client create");
		AdminClient admin = AdminClient.create(consumerProperties);

		System.out.println("debug2 after admin client create");

		//Try to consume few records from each kafka topic in the list
		consumeAndCopyTopic(admin,topics);
		admin.close();

	}


	protected static void consumeAndCopyTopic(AdminClient admin, String[] topics)
	{

		KafkaConsumer<Bytes, Bytes> consumer=null;
		Producer<Bytes, Bytes> producer=null;
		try {
			System.out.println("##########################Copy Topic:################################");
			/*
			 * Consume from each topic
			 * 
			 */

			//consumer configs
			consumerProperties.put("group.id", "ManageTopics9-"+topics[0]);
			consumerProperties.put("enable.auto.commit", "true");
			consumerProperties.put("auto.commit.interval.ms", "1000");
			consumerProperties.put("session.timeout.ms", "30000");

			consumerProperties.put("auto.offset.reset", "earliest");
			consumer = new KafkaConsumer<Bytes,Bytes>(consumerProperties);


			//producer configs
	

			producerProperties.put("acks", "all");
			producerProperties.put("retries", 0);
			producerProperties.put("batch.size", 16384);
			producerProperties.put("linger.ms", 1);
			producerProperties.put("buffer.memory", 33554432);

			producer = new KafkaProducer<Bytes, Bytes>(producerProperties);

			// Subscribe to the source topic
			consumer.subscribe(Arrays.asList(topics[0]));

			int countMessages=0;


			while (true) {
				ConsumerRecords<Bytes, Bytes> records = consumer.poll(100);

				for (ConsumerRecord<Bytes, Bytes> record : records) {
					System.out.printf("%npartition =  %s, offset = %d, key = %s , value = %s", record.partition(), record.offset(), record.key(), record.value());
					System.out.println("position: "+consumer.position(new TopicPartition(topics[0], record.partition())));
					countMessages++;
					System.out.println("MessageCount:"+ countMessages);

					//Copying to the destination topic
					ProducerRecord<Bytes, Bytes> prodRecord = new ProducerRecord<Bytes, Bytes>(topics[1],record.partition() , record.key(),record.value());
					producer.send(prodRecord);
					System.out.printf("%nPublishing to: partition =  %s, key = %s , value = %s", prodRecord.partition(), record.key(), record.value());

				}

			}


		}
		catch(Exception e)
		{

			e.printStackTrace();
		}
		finally
		{
			System.out.println("closing the consumer");
			consumer.close();
			producer.close();

		}
	}

}

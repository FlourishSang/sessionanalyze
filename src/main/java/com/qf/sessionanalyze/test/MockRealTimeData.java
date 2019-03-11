package com.qf.sessionanalyze.test;

import com.qf.sessionanalyze.conf.ConfigurationManager;
import com.qf.sessionanalyze.constant.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	
	private Producer<Integer, String> producer;
	private String brokerList = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);
	private String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);

	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou"});  
		provinceCityMap.put("Hubei", new String[] {"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Zhangjiakou"});
		
		producer = new Producer<Integer, String>(createProducerConfig());
	}
	
	private ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", brokerList);
		return new ProducerConfig(props);
	}
	
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			// 数据格式为：timestamp province city userId adId
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);  
			producer.send(new KeyedMessage<Integer, String>(topics, log));
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}
	
}
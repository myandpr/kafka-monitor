/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bigdata.monitor;

import com.alibaba.fastjson.JSON;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;


public class KafkaServiceImpl  {
	private final static Logger LOG = LoggerFactory.getLogger(KafkaServiceImpl.class);

	/**
	 * Use Kafka low level consumer API to find leader.
	 * 
	 * @param a_seedBrokers
	 * @param a_topic
	 * @param a_partition
	 * @return PartitionMetadata.
	 * @see PartitionMetadata
	 */
	private PartitionMetadata findLeader(List<String> a_seedBrokers, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				String ip = seed.split(":")[0];
				String port = seed.split(":")[1];
				consumer = new SimpleConsumer(ip, Integer.parseInt(port), 10000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest topicMetaReqst = new TopicMetadataRequest(topics);
				TopicMetadataResponse topicMetaResp = consumer.send(topicMetaReqst);

				List<TopicMetadata> topicMetadatas = topicMetaResp.topicsMetadata();
				for (TopicMetadata item : topicMetadatas) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				LOG.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		return returnMetaData;
	}


	/**
	 * Use Kafka low consumer API & get logsize size from zookeeper.
	 *
	 * @param hosts
	 *            Zookeeper host list.
	 * @param topic
	 *            Appoint topic.
	 * @param partition
	 *            Appoint partition.
	 * @return Long.
	 */
	public long getLogSize(List<String> hosts, String topic, int partition) {
		LOG.debug("Find leader hosts [" + hosts + "]");
		//	之所以要获取leader，是因为只有leader才能获取logSize，因为多个副本之间有waterMark线，是三个副本的offset最小值，就是leader上的offset
		//	就像木桶原理一样

		//  注意：找的是某分区的leader，分区副本机制
		PartitionMetadata metadata = findLeader(hosts, topic, partition);
		if (metadata == null) {
			LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find metadata for Topic and Partition. Exiting");
			return 0L;
		}
		if (metadata.leader() == null) {
			LOG.error("[KafkaClusterUtils.getLogSize()] - Can't find Leader for Topic and Partition. Exiting");
			return 0L;
		}

		String clientName = "Client_" + topic + "_" + partition;
		String reaHost = metadata.leader().host();
		int port = metadata.leader().port();

		long ret = 0L;
		try {
			SimpleConsumer simpleConsumer = new SimpleConsumer(reaHost, port, 100000, 64 * 1024, clientName);
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
			

			//	该函数就是和logSize相关的
			OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
			if (response.hasError()) {
				LOG.error("Error fetching data Offset , Reason: " + response.errorCode(topic, partition));
				return 0;
			}
			long[] offsets = response.offsets(topic, partition);
			ret = offsets[0];
			if (simpleConsumer != null) {
				simpleConsumer.close();
			}
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
		}
		return ret;
	}

	//	Kafka在启动时会在zookeeper中/brokers/ids路径下创建一个与当前broker的id为名称的虚节点,Kafka的健康状态检查就依赖于此节点。
	private final String BROKER_IDS_PATH = "/brokers/ids";

	/** Get all broker list from zookeeper. */
	//	构建ZkClient从zookeeper的kafka启动保存的broker路径中获取所有broker
	public List<String> getAllBrokersInfo(String clusterAlias) {

		List list = new ArrayList<String>();
		ZkClient zkc = new ZkClient(clusterAlias, Integer.MAX_VALUE, 100000, ZKStringSerializer$.MODULE$);

		if (ZkUtils.apply(zkc, false).pathExists(BROKER_IDS_PATH)) {
			//	获取zookeeper中路径/brokers/ids下的所有文件名（broker id号命名的文件）
			Seq<String> subBrokerIdsPaths = ZkUtils.apply(zkc, false).getChildren(BROKER_IDS_PATH);
			List<String> brokerIdss = JavaConversions.seqAsJavaList(subBrokerIdsPaths);

			for (String ids : brokerIdss) {
				try {
					//	从每个broker在zookeeper中的路径文件中读取文件内容
					Tuple2<Option<String>, Stat> tuple = ZkUtils.apply(zkc, false).readDataMaybeNull(BROKER_IDS_PATH + "/" + ids);

					String host = JSON.parseObject(tuple._1.get()).getString("host");
					int port = JSON.parseObject(tuple._1.get()).getInteger("port");

					list.add(host+":"+port);

				} catch (Exception ex) {
					LOG.error(ex.getMessage());
				}
			}
		}
		return list;
	}
}

package org.bigdata.monitor;

// caffeine高性能缓存库
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.protocol.types.Type.*;

public class KafkaLagRunning implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLagRunning.class);

    private String zkServer;


    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    // 用于存储在监控的group和topic的映射关系信息,group_id#topic
    Set<String> groupTopicSet = new HashSet<String>();

    private String mails = null;
    private int threshold = 10000;


    /*下面两个函数KafkaLagRunning和setGroup2Topic都是解析参数的*/

    // groupid:topic1#topic2;groupid:topic1#topic2
    // 构造函数
    public KafkaLagRunning(String zkServer,String mails,String groupid_topic,String threshold){
        this.zkServer = zkServer;
        this.mails =mails;
        try {
            setGroup2Topic(groupid_topic);
            this.threshold = Integer.valueOf(threshold);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    // groupid:topic1#topic2;groupid:topic1#topic2
    //  解析封装成一个个的 groupid#topic字符串
    private void setGroup2Topic(String groupid_topics) throws Exception {
        if(groupid_topics==null){
            throw new Exception("groupid_topic must be set !!!");
        }
        String[] gts = groupid_topics.split(";");
        for(String gt:gts){
            String[] g_t = gt.split(":");
            String groupid = g_t[0];
            String topics = g_t[1];
            String[] ts = topics.split("#");
            for(String topic:ts){
                String tmp = groupid+"#"+topic;
                LOG.info("groupTopicSet is added : "+tmp);
                groupTopicSet.add(tmp);
            }

        }
    }

    private Schema schemaValue = new Schema(new Field("offset", INT64), new Field("metadata", STRING, "Associated metadata.", "")
            , new Field("commit_timestamp", INT64), new Field("expire_timestamp", INT64));
    private Schema offsetVersion1 = new Schema(new Field("group", STRING), new Field("topic", STRING), new Field("partition", INT32));
    private Schema offsetVersion2 = new Schema(new Field("group", STRING));
    private Cache<OffsetKey, Long> groupTopicPartitionOffsetMap = Caffeine
            .newBuilder()
            .maximumSize(1025)
            .expireAfterAccess(10, TimeUnit.DAYS).build();

    //  该方法根据buffer，返回一个offsetKey数据结构（类），保存group topic partition
    private OffsetKey readMessageKey(ByteBuffer buffer){

        short version = buffer.getShort();
        if(version == 1){
            OffsetKey offsetKey = new OffsetKey();
            Struct key = offsetVersion1.read(buffer);
            String group = key.getString("group");
            String topic = key.getString("topic");
            Integer partition = key.getInt("partition");

            offsetKey.setGroup(group);
            offsetKey.setTopic(topic);
            offsetKey.setPartition(partition);

            return offsetKey;
        }

        return null;
    }

    //  这一步是读取offset的
    private  Long readMessageValue(ByteBuffer buffer){

        short version = buffer.getShort();
        Struct value = schemaValue.read(buffer);

        if(version == 1){
            Long offset = value.getLong("offset");
            return offset;
        }

        return 0l;
    }

    //  根据kafka所有broker列表，构建一个kafka的消费者consumer
    //  这个consumer不是从zookeeper读数据，是从新版本的kafka的保存offset的topic __consumer_offsets消费数据
    private KafkaConsumer getConsumer(List<String> allBrokersInfo){
        Properties props = new Properties();
        props.put("bootstrap.servers", allBrokersInfo.get(0));
        props.put("group.id", "dataquality"+System.currentTimeMillis()/100000);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        //  订阅topic（__consuer_offsets）
        consumer.subscribe(Arrays.asList("__consumer_offsets"));

        return consumer;
    }

    //  此处开始正式获取lagSize
    public void getLagSizesSimple(String zkServer) throws Exception {

        KafkaServiceImpl kafkaService = new KafkaServiceImpl();
        //  从zookeeper获取kafka集群所有broker信息，返回值是host:port格式字符串
        List<String> allBrokersInfo = kafkaService.getAllBrokersInfo(zkServer);

        //  创建一个kafka的consumer去订阅偏移量的__consumer_offsets这个topic
        KafkaConsumer consumer = getConsumer(allBrokersInfo);

        LOG.info("zookeeper server is " +zkServer);
        while (true) {

            try {
                // 保证1秒从kafka取一次数据，否则consumer会在10秒中内关闭，poll的参数1000指的是等待时间1秒
                ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
                if (records.isEmpty() ) {
                    continue;
                }

                //  保存该批次的groupTopicPartitionOffsetMap.put(offsetKey, offset)，
                //  保存该批次每个topic-partition最大的offset
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.value() != null) {
                        //  读取groupid-topic-partition
                        OffsetKey offsetKey = readMessageKey(ByteBuffer.wrap(record.key()));

                        if (offsetKey != null) {
                            String groupId = offsetKey.getGroup();
                            String topic = offsetKey.getTopic();

                            String tmp = groupId+"#"+topic;

                            // 判断是否是要监控的group和topic，如果没有的话，就去看该批次的吓一条record包含不包含
                            if(!groupTopicSet.contains(tmp)){
                                continue;
                            }

                            LOG.info("offsetKey is " +offsetKey);

                            Long offset = readMessageValue(ByteBuffer.wrap(record.value()));
                            if (offset != 0) {
                                // 一个批次消费的records打印的offset是根据topic-partition按offset从小到大顺序打印的(不包括groupid)，
                                //  所以offset越来越多，越往后越大，而groupTopicPartitionOffsetMap是update or insert方式
                                //  https://www.cnblogs.com/muxi0407/p/11794799.html
                                groupTopicPartitionOffsetMap.put(offsetKey, offset);
                            }
                        }
                    }
                }

                //  下面就是对该一分钟最后一批次计算出的groupTopicPartitionOffsetMap.put(offsetKey, offset)计算

                Long date = Long.valueOf(sdf.format(new Date()));
                // 一分钟更新一次
                if(date % 100 == 0) {

                    Map<String, Long> lagSizeMap = new HashMap<String, Long>();
                    Map<OffsetKey, Long> offsetKeyLongMap = groupTopicPartitionOffsetMap.asMap();
                    for (Map.Entry<OffsetKey, Long> entry : offsetKeyLongMap.entrySet()) {
                        OffsetKey offsetKey = entry.getKey();
                        String groupId = offsetKey.getGroup();
                        String topic = offsetKey.getTopic();


                        String lagSizeKey =topic + "###" + groupId;

                        LOG.info("KafkaLagRunning.getLagSizesSimple topic=" + topic + ", groupId=" + groupId );

                        //  第一步：获取每个topic-partition的offset
                        Long offset = entry.getValue();
                        //  第二步： 获取每个topic-partition的logSize
                        long logSize = kafkaService.getLogSize(allBrokersInfo, topic, offsetKey.getPartition());

                        LOG.info("LogSize is : "+logSize);
                        LOG.info("currnet offset  is : "+offset);

                        //  表示生产比消费快
                        if (logSize >= offset) {
                            long lagsize = logSize - offset;

                            // 会累积所有分区的lagsize，然后计算总的lagsize
                            //  最终获取的是每个groupid###topic的总lagSize，而不是每个partition的lagSize
                            if (lagSizeMap.containsKey(lagSizeKey)){
                                Long newLagSize = lagSizeMap.get(lagSizeKey) + lagsize;
                                lagSizeMap.put(lagSizeKey, newLagSize);
                            } else {
                                lagSizeMap.put(lagSizeKey, lagsize);
                            }
                        }

                    }

                    //  这里是告警，当lagSize大于threshold这个阈值，就告警
                    for (Map.Entry<String, Long> entry : lagSizeMap.entrySet()) {

                        try {

                            Long lagSize = entry.getValue();

                            LOG.info("current LagSize is : "+lagSize );

                            if (lagSize > this.threshold) {
                                LOG.warn("LagSize beyonds the threshold!!! Please handle it!");

                            }
                        }catch (Exception ex){
                            LOG.error("KafkaLagRunning.getLagSizesSimple", ex);
                        }
                    }
                }
            } catch (Exception ex){
                LOG.error("KafkaLagRunning.getLagSizesSimple", ex);
                throw ex;
            }
        }
    }
    public void run() {
        try {
            getLagSizesSimple(zkServer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

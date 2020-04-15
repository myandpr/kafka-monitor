package org.bigdata.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class KafkaLagMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLagMonitor.class);

    public static void main(String [] args) throws Exception {

        try {
            String confPath = System.getProperty("base.config.dir");
            LOG.info("kafka config path is : "+confPath);

            InputStream inStream = new FileInputStream(new File(confPath+"/kafka.properties"));

            Properties prop = new Properties();
            prop.load(inStream);

            String zkservers = prop.getProperty("kafka.zookeeper", "localhost:2181");
            String threshold = prop.getProperty("lagsize.threshold", "100");
            String mails = prop.getProperty("mail.list", "111@163.com");
            String group_topics = prop.getProperty("kafka.groupid-topics", "test:test1#test2");

            //传入要监控的信息
            KafkaLagRunning lagRunning = new KafkaLagRunning(zkservers, mails, group_topics, threshold);
            LOG.info("start running!!!!");
            new Thread(lagRunning).start();
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            throw e;
        }

    }

}

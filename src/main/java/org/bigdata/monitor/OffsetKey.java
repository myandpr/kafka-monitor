package org.bigdata.monitor;

import java.util.Objects;

public class OffsetKey {
    private String  topic;
    private String group;
    private Integer partition;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "OffsetKey{" +
                "topic='" + topic + '\'' +
                ", group='" + group + '\'' +
                ", partition=" + partition +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetKey offsetKey = (OffsetKey) o;
        return Objects.equals(topic, offsetKey.topic) &&
                Objects.equals(group, offsetKey.group) &&
                Objects.equals(partition, offsetKey.partition);
    }

    @Override
    public int hashCode() {

        return Objects.hash(topic, group, partition);
    }
}

package org.bigdata.monitor;

import java.util.Objects;

public class TopicGroupKey {
    private String  topic;
    private String group;

    public TopicGroupKey(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }

    public TopicGroupKey() {
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicGroupKey that = (TopicGroupKey) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {

        return Objects.hash(topic, group);
    }

    @Override
    public String toString() {
        return "TopicGroupKey{" +
                "topic='" + topic + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}

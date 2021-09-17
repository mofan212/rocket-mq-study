package indi.mofan.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息生产者
 *
 * @author mofan
 * @date 2021/9/16 23:40
 */
public class BatchProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        // 指定要发送消息的最大大小，默认是 4M。同时需要修改 Broker 配置文件的 maxMessageSize 属性
        producer.setMaxMessageSize(8 * 1024 * 1024);
        producer.start();

        // 定义要发送的消息集合
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("someTopic", "someTag", body);
            messages.add(message);
        }

        // 定义消息列表分割器，将消息列表分割为多个不超过 4M 的小列表
        MessageListSplitter splitter = new MessageListSplitter(messages);
        while (splitter.hasNext()) {
            List<Message> next = splitter.next();
            producer.send(next);
        }

        producer.shutdown();
    }
}

package indi.mofan.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * 单向消息发送生产者
 *
 * @author mofan
 * @date 2021/9/12 22:01
 */
public class OneWayProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("single", "someTag", body);
            // 单向发送
            producer.sendOneway(message);
        }

        producer.shutdown();
        System.out.println("producer shutdown");
    }
}

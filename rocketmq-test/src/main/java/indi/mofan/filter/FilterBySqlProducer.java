package indi.mofan.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 定义SQL过滤Producer
 *
 * @author mofan
 * @date 2021/9/18 22:34
 */
public class FilterBySqlProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TopicE", "myTag", body);
            // 事先埋入用户属性 age
            message.putUserProperty("age", i + "");
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}

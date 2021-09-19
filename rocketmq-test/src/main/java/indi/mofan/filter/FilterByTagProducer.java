package indi.mofan.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 定义Tag过滤Producer
 *
 * @author mofan
 * @date 2021/9/18 22:26
 */
public class FilterByTagProducer {
    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        producer.start();
        String[] tags = {"myTagA", "myTagB", "myTagC"};
        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi" + i).getBytes();
            String tag = tags[i % tags.length];
            Message message = new Message("myTopic", tag, body);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}

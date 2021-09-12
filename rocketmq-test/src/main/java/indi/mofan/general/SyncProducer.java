package indi.mofan.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 同步消息发送生产者
 *
 * @author mofan
 * @date 2021/9/12 17:17
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 创建一个 Producer，参数为 Producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 指定 NameServer 的地址
        producer.setNamesrvAddr("rocketmq:9876");
        // 设置当发送失败时重试发送的次数，默认为 2 次
        producer.setRetryTimesWhenSendAsyncFailed(3);
        // 设置发送超时时限，默认为 3s
        producer.setSendMsgTimeout(5000);
        // 开启生产者
        producer.start();

        // 生产者发送 100 条信息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("someTopic", "someTag", body);
            // 为消息指定 key
            message.setKeys("key-" + i);
            // 发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        // 关闭 Producer
        producer.shutdown();
    }
}

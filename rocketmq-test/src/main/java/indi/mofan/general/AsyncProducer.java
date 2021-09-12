package indi.mofan.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 异步消息发送生产者
 *
 * @author mofan
 * @date 2021/9/12 21:45
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        // 指定异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 指定新创建的 Topic 的 Queue 数量为 2，默认为 4
        producer.setDefaultTopicQueueNums(2);
        producer.start();

        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("myTopicA", "myTag", body);
            // 异步发送，指定回调
            producer.send(message, new SendCallback() {
                // 当 producer 接收到 MQ 发送来的 ACK 后就会触发该回调方法的执行
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
        }

        // 线程休眠
        /* 由于采用的是异步发送，若不进行休眠，消息还未发送就会将 producer 关闭而导致报错 */
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }
}

package indi.mofan.retry;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息重试消费者
 *
 * @author mofan
 * @date 2021/9/19 23:03
 */
public class RetryConsumer {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        consumer.setNamesrvAddr("rocketmq:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("someTopic", "");
        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (messageList, consumeConcurrentlyContext) -> {
            try {
                for (MessageExt msg : messageList) {
                    System.out.println(msg);
                }
            } catch (Throwable e) {
                // 以下三种情况均可引发消息重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                /* return null;
                   return new RuntimeException("消息异常"); */
            }
            // 返回消费状态，消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer Started");
    }
}

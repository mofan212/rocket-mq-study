package indi.mofan.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 批量消息消费者
 *
 * @author mofan
 * @date 2021/9/17 22:02
 */
public class BatchConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        consumer.setNamesrvAddr("rocketmq:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("someTopic", "*");

        // 指定每次可以消费 10 条消息，默认为 1
        consumer.setConsumeMessageBatchMaxSize(10);
        // 指定每次可以从 Broker 中拉取 40 条消息，默认为 32
        consumer.setPullBatchSize(40);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println(msg);
            }
            // 消费成功的返回结果
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            // 消费失败的返回结果
            /* return ConsumeConcurrentlyStatus.RECONSUME_LATER; */
        });
        consumer.start();
        System.out.println("Consumer Started");
    }
}

package indi.mofan.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 顺序消息
 *
 * @author mofan
 * @date 2021/9/13 0:23
 */
public class OrderedProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        // 若使用全局有序，需要设置 Queue 的数量为 1
        /* producer.setDefaultTopicQueueNums(1); */

        producer.start();

        for (int i = 0; i < 100; i++) {
            // 仅为了演示使用整型作为 orderId
            Integer orderId = i;
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TopicA", "TagA", body);
            // 将 orderId 作为消息 Key
            message.setKeys(orderId.toString());
            // send() 方法的第三个参数会传递给选择器方法的第三个参数，即 Lambda 表达式中的参数 o
            SendResult sendResult = producer.send(message, (list, msg, o) -> {
                /* 以下为具体的选择算法 */

                // 以下是使用消息 Key 作为选择 Key 的选择算法
                int id = Integer.parseInt(message.getKeys());

                // 以下是使用参数 o 作为选择 Key 的选择算法
                /* Integer id = (Integer) o; */
                int index = id % list.size();
                return list.get(index);
            }, orderId);

            System.out.println(sendResult);
        }

        producer.shutdown();

    }
}

package indi.mofan.transaction;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 事务消息生产者
 *
 * @author mofan
 * @date 2021/9/15 23:38
 */
public class TransactionProducer {
    public static void main(String[] args) throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("tpg");
        producer.setNamesrvAddr("rocketmq:9876");

        // 定义一个线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), (Runnable r) -> {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        });

        // 为生产者指定一个线程池
        producer.setExecutorService(threadPoolExecutor);
        // 为生产者添加事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());
        producer.start();

        String[] tags = {"TAGA", "TAGB", "TAGC"};
        for (int i = 0; i < 3; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TTopic", tags[i], body);
            // 发送事务消息。第二个参数用于指定在执行本地事务时要使用的业务参数
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("发送结果为：" + sendResult.getSendStatus());
        }
    }
}

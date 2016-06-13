package broker;

import backup.BackupTasks;
import models.Message;
import models.Subscriber;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by r on 13.06.16.
 */
public class SubscribersManageTest {
    private static SubscribersManage subscribersManage;
    private static BackupTasks backupTasks;
    private static Subscriber firstSubscriber;

    @BeforeClass
    public static void beforeClass() {
        backupTasks = mock(BackupTasks.class);
        doNothing().when(backupTasks).addMessage(anyString(), any(Message.class));
    }

    @Before
    public void setUpSubscribersManage() {
        subscribersManage = new SubscribersManage();
        firstSubscriber = new Subscriber("foo", "http://localhost:18080/save");
        firstSubscriber.getTopics().add("1");
        subscribersManage.getSubscribersList().add(firstSubscriber);
    }


    @Test
    public void oneMessage() {
        Message message1 = new Message("1", "1", new Timestamp((new Date()).getTime()));
        subscribersManage.addMessage(backupTasks, message1);
        Message actualMessage = subscribersManage.getSubscribersList().get(0).getMessageQueue().poll();
        assertSame(message1, actualMessage);
    }

    @Test
    public void concurrencyMessages() throws BrokenBarrierException, InterruptedException {
        int threadPoolSize = 4;
        int messagesSize = threadPoolSize * 10;
        List<Message> messages = new ArrayList<>(messagesSize);
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        final CyclicBarrier barrier = new CyclicBarrier(threadPoolSize);
        for (int i = 0; i < messagesSize; i++) {
            Message message = new Message(String.valueOf(i), "1", new Timestamp((new Date()).getTime()));
            messages.add(message);
            executor.execute(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
                subscribersManage.addMessage(backupTasks, message);
            });
        }

        Thread.sleep(4 * 1000);

        Queue<Message> queue = subscribersManage.getSubscribersList().get(0).getMessageQueue();
        assertEquals(messagesSize, queue.size());
        for (Message message : messages) {
            assertTrue(queue.contains(message));
        }
    }

    @Test
    public void orderTest() {
        int messagesSize = 10;
        List<Message> messages = new ArrayList<>(messagesSize);
        for (int i = 0; i < messagesSize; i++) {
            Message message = new Message(String.valueOf(i), "1", new Timestamp((new Date()).getTime()));
            messages.add(message);
            subscribersManage.addMessage(backupTasks, message);
        }

        Queue<Message> queue = subscribersManage.getSubscribersList().get(0).getMessageQueue();
        assertEquals(messagesSize, queue.size());
        int i = 0;
        for (Message message : queue) {
            assertSame(messages.get(i), message);
            i++;
        }

    }

}
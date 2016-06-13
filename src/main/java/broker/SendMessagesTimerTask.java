package broker;

import backup.BackupTasks;
import models.Message;
import models.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * раз в 3 секунды, проходится по очередям сообщений всех подписчиков, и если они имеют сообщения, то отправляет их.
 * Если сообщения успешно доставлены, то помечает в бд, какое последнее сообщение было успешно доставлено.
 * Лок для каждого подписчика нужен, чтобы при долгой отправке сообщений, эти же сообщения не добавились на отправку в другом потоке через следующие 3 секунды.
 * Created by r on 10.06.16.
 */
public class SendMessagesTimerTask extends TimerTask {
    private ExecutorService executor = Executors.newFixedThreadPool(4);
    private SubscribersManage subscribersManage;
    private BackupTasks backupTasks;
    private static Logger logger = LoggerFactory.getLogger(SendMessagesTimerTask.class);

    public SendMessagesTimerTask(SubscribersManage subscribersManage, BackupTasks backupTasks) {
        this.subscribersManage = subscribersManage;
        this.backupTasks = backupTasks;
    }

    @Override
    public void run() {
        for (Subscriber subscriber : subscribersManage.getSubscribersList()) {

            executor.execute(() -> {
                if (subscriber.lockInSendMessage.tryLock()) {
                    try {
                        // отправить все сообщения из очереди
                        int countForDelete = sendPOST(subscriber.getUrl(), subscriber.getMessageQueue());
                        // при успешной попытке отправки, удалить отправленные сообщения из очереди
                        Message lastMessage = null;
                        for (int i = 0; i < countForDelete; i++) {
                            lastMessage = subscriber.getMessageQueue().poll();
                        }
                        //помечаем в бд последнее переданное сообщение
                        if (lastMessage != null) {
                            backupTasks.markLastSentMessage(subscriber.getSubscriberName(), lastMessage);
                        }

                    } finally {
                        subscriber.lockInSendMessage.unlock();
                    }
                }
            });
        }

    }

    // возвращает количество успешно отправленных сообщений, чтобы удалить их из очереди
    private static int sendPOST(String subscriberUrl, Queue<Message> messages) {
        if (messages.size() == 0) {
            return 0;
        }
        StringBuilder body = new StringBuilder();
        int countForDelete = 0;// м.б. есть смысл заменить на size(), если другие потоки не могут добавлять
        //ConcurrentModificationException не страшен
        for (Message message : messages) {
            body.append("message=");
            body.append(message.getMessage());
            body.append("&topic=");
            body.append(message.getTopic());
            body.append("&");
            countForDelete++;
        }


        try {
            URL url = new URL(subscriberUrl);

            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);

            try (OutputStream os = con.getOutputStream()) {
                os.write(body.toString().getBytes());
                os.flush();
                os.close();
            }


            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                return countForDelete;
            }

        } catch (IOException e) {
            logger.warn("Subscriber does not receive messages.");
            //return 0;
        }

        return 0;

    }

    public void shutdown() {
        executor.shutdown();
    }

}

package backup;

import models.Message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by r on 12.06.16.
 */
public class BackupTasks {
    private DBService dbService;
    private ExecutorService executor = Executors.newFixedThreadPool(1);

    public BackupTasks(DBService dbService) {
        this.dbService = dbService;
    }

    public void initSubscriber(String subscriberName, String url) {
        executor.submit(() -> {
            dbService.initSubscriber(subscriberName, url);
        });
    }

    public void markLastSentMessage(String subscriberName, Message lastMessage) {
        executor.submit(() -> {
            dbService.markLastSentMessage(subscriberName, lastMessage);
        });
    }

    public void addMessage(String subscriberName, Message message) {
        executor.submit(() -> {
            dbService.addMessage(subscriberName, message);
        });
    }

    public void addTopic(String topicName) {
        executor.submit(() -> {
            dbService.addTopic(topicName);
        });
    }

    public void shutdown(){
        executor.shutdown();
    }
}

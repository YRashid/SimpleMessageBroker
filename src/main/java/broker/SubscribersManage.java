package broker;

import backup.BackupTasks;
import models.Message;
import models.Subscriber;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by r on 09.06.16.
 */
public class SubscribersManage {
    private List<Subscriber> subscribersList = new CopyOnWriteArrayList<>();
    private static final long MAX_FREE_MEMORY = 1000000;

    //TODO написать чем регулируется freeMemory
    public boolean addMessage(BackupTasks backupTasks, Message message) {
        // Если очередь забилась (слушатели не успевают разгребать), то отправляем ответ писателю,
        //  что сообщение не будет доставлено и просьбой попробовать позже с кодом 500.
        if (Runtime.getRuntime().freeMemory() - MAX_FREE_MEMORY < 0) {
            return false;
        }


        for (Subscriber subscriber : subscribersList) {
            Set<String> topics = subscriber.getTopics();
            if (topics.contains(message.getTopic())) {
                subscriber.getMessageQueue().add(message);
                backupTasks.addMessage(subscriber.getSubscriberName(), message);
            }
        }

        return true;
    }

    // Получить подписчика из списка по имени
    public Subscriber getSubscriberBySubscriberName(String subscriberName) {
        for (Subscriber subscriber : subscribersList) {
            if (subscriber.getSubscriberName().equals(subscriberName)) {
                return subscriber;
            }
        }
        return null;
    }

    public List<Subscriber> getSubscribersList() {
        return subscribersList;
    }
}

package broker;

import backup.BackupTasks;
import models.Message;
import models.Subscriber;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * subscribersList - список всех зарегистрированных подписчиков.
 * Метод addMessage(BackupTasks backupTasks, Message message) добавляет переданное сообщение
 *  в очереди сообщений тех подписчиков, кто подписан на топик этого сообщения.
 *
 * Created by r on 09.06.16.
 */
public class SubscribersManage {
    private List<Subscriber> subscribersList = new CopyOnWriteArrayList<>();
    private static final long MIN_FREE_MEMORY = 1000000;

    public boolean addMessage(BackupTasks backupTasks, Message message) {
        //TODO написать как увеличить память
        // Если очередь забилась (слушатели не успевают разгребать), то отправляем ответ писателю,
        //  что сообщение не будет доставлено и просьбой попробовать позже с кодом 500.
        if (Runtime.getRuntime().freeMemory() - MIN_FREE_MEMORY < 0) {
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

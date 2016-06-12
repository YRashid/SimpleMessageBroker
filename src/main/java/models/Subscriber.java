package models;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by r on 09.06.16.
 */
// TODO: переимновать в Subscriber, если не вынесу subscriberName в key мапы
public class Subscriber {
    private final String subscriberName;
    private Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private Set<String> topics = new ConcurrentSkipListSet<>();
    private final String url;
    public Lock lockInSendMessage = new ReentrantLock();
    ;

    public Subscriber(String subscriberName, String url) {
        this.subscriberName = subscriberName;
        this.url = url;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public Queue<Message> getMessageQueue() {
        return messageQueue;
    }

    public String getUrl() {
        return url;
    }

    public Set<String> getTopics() {
        return topics;
    }
}

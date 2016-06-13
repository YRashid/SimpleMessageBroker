package models;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by r on 10.06.16.
 */
public class Message {
    private final String message;
    private final String topic;
    private final Timestamp date;

    public Message(String message, String topic, Timestamp date) {
        this.message = message;
        this.topic = topic;
        this.date = date;
    }

    public Timestamp getDate() {
        return date;
    }

    public String getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }

}

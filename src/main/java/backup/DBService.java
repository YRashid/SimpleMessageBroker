package backup;

import models.Message;
import models.Subscriber;
import broker.SubscribersManage;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TODO: добавить обработку ошибок
 * TODO: надо подчищать бд
 * Необязательно все время передавать в бд имя топика, можно хранить имя топика в струкрутре в памяти
 * и синхронизировать с таблицей в бд. Тогда можно будет передавать сразу id топика. Аналогично с именем подписчика.
 * Created by r on 12.06.16.
 */
public class DBService {
    private static Logger logger = LoggerFactory.getLogger(DBService.class);
    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:./backup";
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";
    private static JdbcConnectionPool connectionPool;

    public DBService(SubscribersManage subscribersManage) {
        connectionPool = getConnectionPool();
        createTablesIfNotExist();

        // Загрузить всех слушателей из бд. Всех - т.к. операции отписки нет.
        List<Subscriber> subscribers = getSubscribers();
        for (Subscriber subscriber : subscribers) {
            Set<String> topics = getSubscriberTopics(subscriber.getSubscriberName());
            subscriber.getTopics().addAll(topics);
            List<Message> newMessages = getSubscriberNewMessages(subscriber.getSubscriberName());
            for (Message message : newMessages) {
                subscriber.getMessageQueue().add(message);
            }
            subscribersManage.getSubscribersList().add(subscriber);
        }

    }

    //TODO: указать, макс размер сообщения и топика
    // создание таблиц: меток последнего сообщения, топиков и всех сообщений
    private void createTablesIfNotExist() {
        final String createTopicsTableQuery = "CREATE TABLE IF NOT EXISTS topics (id int primary key AUTO_INCREMENT, name varchar(128) UNIQUE)";
        final String createSubscribersTableQuery = "CREATE TABLE IF NOT EXISTS subscribers (id int primary key AUTO_INCREMENT, subscriberName varchar(128) UNIQUE, url varchar(1024) NOT NULL, lastMessageDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        final String createMessagesTableQuery = "CREATE TABLE IF NOT EXISTS messages (id int primary key AUTO_INCREMENT, message varchar(5000), subscriberId int, topicId int, messageDate TIMESTAMP NOT NULL, FOREIGN KEY (subscriberId) REFERENCES subscribers(id), FOREIGN KEY (topicId) REFERENCES topics(id))";
        final String createSubscribersTopicsTableQuery = "CREATE TABLE IF NOT EXISTS subscribers_topics (id int primary key AUTO_INCREMENT, topicId int, subscriberId int, FOREIGN KEY (subscriberId) REFERENCES subscribers(id), FOREIGN KEY (topicId) REFERENCES topics(id))";
        try (Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);

            Statement stmt = connection.createStatement();
            stmt.execute(createTopicsTableQuery);
            stmt.execute(createSubscribersTableQuery);
            stmt.execute(createMessagesTableQuery);
            stmt.execute(createSubscribersTopicsTableQuery);

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
    }

    // Получить все топики подписчика
    private Set<String> getSubscriberTopics(String subscriberName) {
        Set<String> result = new HashSet<>();
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT topics.name FROM topics, subscribers, subscribers_topics WHERE subscribers.subscriberName=? AND subscribers.id=subscribers_topics.subscriberId AND subscribers_topics.topicId=topics.id");
            stmt.setString(1, subscriberName);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
        return result;
    }

    // Получить все не отправленные сообщения
    private List<Message> getSubscriberNewMessages(String subscriberName) {
        List<Message> result = new ArrayList<>();
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT messages.message, topics.name, messages.messageDate  FROM topics, messages, subscribers WHERE subscribers.subscriberName=? AND subscribers.id=messages.subscriberId AND messages.topicId=topics.id AND messages.messageDate>subscribers.lastMessageDate ORDER BY messages.messageDate");
            stmt.setString(1, subscriberName);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Message message = new Message(resultSet.getString(1), resultSet.getString(2), resultSet.getTimestamp(3));
                result.add(message);
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
        return result;
    }

    private List<Subscriber> getSubscribers() {
        List<Subscriber> result = new ArrayList<>();
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT subscriberName, url FROM subscribers");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Subscriber subscriber = new Subscriber(resultSet.getString(1), resultSet.getString(2));
                result.add(subscriber);
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
        return result;
    }

    //Создать поле для записи последнего сообщения нового подписчика
    public void initSubscriber(String subscriberName, String url) {
        final String query = "INSERT INTO subscribers (subscriberName, url) values (?, ?)";
        PreparedStatement stmt;
        try (Connection connection = connectionPool.getConnection()) {
            stmt = connection.prepareStatement(query);
            stmt.setString(1, subscriberName);
            stmt.setString(2, url);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
    }

    //Создать новый топик и добавить его подписчику
    public void addTopic(String topicName, String subscriberName) {
        int topicId = getTopicIdByTopic(topicName);


        final String createTopic = "INSERT INTO topics (name) values (?)";
        final String linkSubscriberTopic = "INSERT INTO subscribers_topics (topicId, subscriberId) SELECT topics.id, subscribers.id FROM topics, subscribers WHERE topics.name=? AND subscribers.subscriberName=?";
        PreparedStatement stmt;
        try (Connection connection = connectionPool.getConnection()) {
            connection.setAutoCommit(false);
            if (topicId == -1) {
                stmt = connection.prepareStatement(createTopic);
                stmt.setString(1, topicName);
                stmt.executeUpdate();
            }

            stmt = connection.prepareStatement(linkSubscriberTopic);
            stmt.setString(1, topicName);
            stmt.setString(2, subscriberName);
            stmt.executeUpdate();

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
    }

    //Добавить сообщение в таблицу сообщений подписчика
    public void addMessage(String subscriberName, Message message) {
        int topicId = getTopicIdByTopic(message.getTopic());
        int subscriberId = getSubscriberIdBySubscriberName(subscriberName);

        final String query = "INSERT INTO messages (subscriberId, topicId, message, messageDate) values (?, ?, ?, ?)";
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setInt(1, subscriberId);
            stmt.setInt(2, topicId);
            stmt.setString(3, message.getMessage());
            stmt.setTimestamp(4, message.getDate());
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
    }

    //Пометить последнее переданное сообщение подписчика. message - последнее переданное сообщение из очереди.
    public void markLastSentMessage(String subscriberName, Message message) {

        final String query = "UPDATE subscribers SET lastMessageDate=? WHERE subscriberName=?";
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setTimestamp(1, message.getDate());
            stmt.setString(2, subscriberName);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
    }

    //TODO: можно объединить следующие два похожих метода
    // Получить id топика подписчика по тексту топика
    private int getTopicIdByTopic(String topic) {
        final String query = "SELECT id FROM topics WHERE name=?";
        int topicId = -1;
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, topic);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                topicId = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
        return topicId;
    }

    // Получить id подписчика (id строчки в которой хранится id последнего сообщения подписчика) по имени подписчика.
    private int getSubscriberIdBySubscriberName(String subscriberName) {
        final String query = "SELECT id FROM subscribers WHERE subscriberName=?";
        int subscriberId = -1;
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
            stmt.setString(1, subscriberName);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                subscriberId = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }
        return subscriberId;
    }

    //TODO: to delete
    public void showAll() {
        System.out.println("Messages: ");
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT id, message, subscriberId, topicId, messageDate FROM messages");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                System.out.println("id: " + resultSet.getString(1) + ";  message: " + resultSet.getString(2) + "; subsId: " + resultSet.getString(3) + ";  topicId: " + resultSet.getString(4) + ";  messageDate: " + resultSet.getString(5));
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }

        System.out.println("\nsubscribers:");
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT id, subscriberName, url, lastMessageDate FROM subscribers");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                System.out.println("id: " + resultSet.getString(1) + ";  subsName: " + resultSet.getString(2) + ";  url: " + resultSet.getString(3) + ";  lastMessageDate: " + resultSet.getString(4));
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }

        System.out.println("\ntopics:");
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT id, name FROM topics");
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                System.out.println("id: " + resultSet.getString(1) + ";  name: " + resultSet.getString(2));
            }
        } catch (SQLException e) {
            logger.error("SQLException: ", e);
        }

    }

    // Create H2 JdbcConnectionPool
    private JdbcConnectionPool getConnectionPool() {
        JdbcConnectionPool cp = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            logger.error("SQLException: ", e);
        }
        cp = JdbcConnectionPool.create(DB_CONNECTION, DB_USER, DB_PASSWORD);
        return cp;
    }
}

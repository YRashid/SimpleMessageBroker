package servlets;

import backup.BackupTasks;
import models.Subscriber;
import broker.SubscribersManage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Сервлет принимающий /subscribe запросы. Создает очередь сообщений для нового подписчика.
 * Добавляет новый топик подписчику. Ставит задачу на добавление нового подписчика в бд.
 *
 * Created by r on 08.06.16.
 */
public class SubscribeServlet extends HttpServlet {
    private static Logger logger = LoggerFactory.getLogger(SubscribeServlet.class);
    private SubscribersManage subscribersManage;
    private BackupTasks backupTasks;


    public SubscribeServlet(SubscribersManage subscribersManage, BackupTasks backupTasks) {
        this.subscribersManage = subscribersManage;
        this.backupTasks = backupTasks;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html; charset=UTF-8");
        String subscriberName = req.getParameter("subscriberName");
        String topic = req.getParameter("topic");
        // url игнорируется, если подписчик существует, но передавать все равно обязательно.
        String url = req.getParameter("url");
        if (url == null) {
            url = req.getParameter("URL");
        }

        if (topic == null || subscriberName == null || url == null) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().print("Topic and subscriberName and url must be not empty.Topic: " + topic + "; subscriberName: " + subscriberName + "; url: " + url);
            logger.info("Topic: {} and subscriberName: {} and url: {} must be not empty.", new Object[]{topic, subscriberName, url});
            return;
        }


        //TODO: ИСПРАВИТЬ - поиск и добавление должны быть атомарными
        Subscriber subscriber = subscribersManage.getSubscriberBySubscriberName(subscriberName);
        // создать очередь для нового подписчика
        if (subscriber == null) {
            logger.info("New subscriber. Name: {}; Url: {}; Topic: {}", new Object[]{subscriberName, url, topic});
            subscriber = new Subscriber(subscriberName, url);
            subscribersManage.getSubscribersList().add(subscriber);
            backupTasks.initSubscriber(subscriberName, url);
        }

        subscriber.getTopics().add(topic);
        backupTasks.addTopic(topic, subscriberName);
//        resp.getWriter().print("topic: " + topic + " url:" + url + " subscriberName:" + subscriberName);
        resp.setStatus(HttpServletResponse.SC_OK);
    }
}

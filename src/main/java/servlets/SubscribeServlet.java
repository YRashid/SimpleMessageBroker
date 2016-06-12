package servlets;

import backup.BackupTasks;
import models.Subscriber;
import broker.SubscribersManage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by r on 08.06.16.
 */
public class SubscribeServlet extends HttpServlet {
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
        String url = req.getParameter("url");
        if (url == null) {
            url = req.getParameter("URL");
        }

        Subscriber subscriber = subscribersManage.getSubscriberBySubscriberName(subscriberName);
        // создать очередь для нового подписчика
        if (subscriber == null) {
            subscriber = new Subscriber(subscriberName, url);
            subscribersManage.getSubscribersList().add(subscriber);
            backupTasks.initSubscriber(subscriberName, url);
        }

        subscriber.getTopics().add(topic);
        backupTasks.addTopic(topic);
        resp.getWriter().print("topic: " + topic + " url:" + url + " subscriberName:" + subscriberName);
        resp.setStatus(HttpServletResponse.SC_OK);
    }
}

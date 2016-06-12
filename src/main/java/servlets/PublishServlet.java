package servlets;

import backup.BackupTasks;
import models.Message;
import broker.SubscribersManage;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by r on 08.06.16.
 */
public class PublishServlet extends HttpServlet {
    private SubscribersManage subscribersManage;
    private BackupTasks backupTasks;

    public PublishServlet(SubscribersManage subscribersManage, BackupTasks backupTasks) {
        this.subscribersManage = subscribersManage;
        this.backupTasks = backupTasks;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html; charset=UTF-8");
        String messageText = req.getParameter("message");
        String topic = req.getParameter("topic");
        if (topic == null || messageText == null) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().print("Topic and message must be not empty. topic:" + topic + ", message:" + messageText);
            return;
        }

        Message message = new Message(messageText, topic, new Timestamp((new Date()).getTime()));
        boolean isAdded = subscribersManage.addMessage(backupTasks, message);
        if (!isAdded) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().print("The queue is overloaded. The message will not be delivered, please send again the message.");
            return;
        }
        resp.setStatus(HttpServletResponse.SC_OK);


    }
}

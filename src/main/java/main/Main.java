package main;

import backup.BackupTasks;
import backup.DBService;
import broker.SubscribersManage;
import broker.SendMessagesTimerTask;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import servlets.PublishServlet;
import servlets.SubscribeServlet;

import java.util.Timer;

/**
 * Created by r on 08.06.16.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        SubscribersManage subscribersManage = new SubscribersManage();
        DBService dbService = new DBService(subscribersManage);
        BackupTasks backupTasks = new BackupTasks(dbService);
        SendMessagesTimerTask sendMessagesTimerTask = new SendMessagesTimerTask(subscribersManage, backupTasks);
        //todo: to delete
        dbService.showAll();

        PublishServlet publishServlet = new PublishServlet(subscribersManage, backupTasks);
        SubscribeServlet subscribeServlet = new SubscribeServlet(subscribersManage, backupTasks);

        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(new ServletHolder(publishServlet), "/publish");
        context.addServlet(new ServletHolder(subscribeServlet), "/subscribe");

        //TODO: добавить возможность выбора порта из аргументов
        Server server = new Server(8080);
        server.setHandler(context);
        server.start();

        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(sendMessagesTimerTask, 0, 2 * 1000);

        server.join();
    }
}

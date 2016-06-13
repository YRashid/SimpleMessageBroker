package main;

import backup.BackupTasks;
import backup.DBService;
import broker.SubscribersManage;
import broker.SendMessagesTimerTask;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import servlets.PublishServlet;
import servlets.SubscribeServlet;

import java.util.Timer;

/**
 * Стартует при запуске проекта. Поднимает веб-сервер, на переданном порту (по умолчанию 8080).
 * Инициализирует все необходимые классы, запускает таймер - выполняющий передачу новых сообщений подписчикам.
 * <p>
 * Created by r on 08.06.16.
 */
public class Main {
    private static int port = 8080;
    private static Logger logger = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {
        SubscribersManage subscribersManage = new SubscribersManage();
        DBService dbService = new DBService(subscribersManage);
        BackupTasks backupTasks = new BackupTasks(dbService);
        SendMessagesTimerTask sendMessagesTimerTask = new SendMessagesTimerTask(subscribersManage, backupTasks);
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(sendMessagesTimerTask, 0, 3 * 1000);

        PublishServlet publishServlet = new PublishServlet(subscribersManage, backupTasks);
        SubscribeServlet subscribeServlet = new SubscribeServlet(subscribersManage, backupTasks);

        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(new ServletHolder(publishServlet), "/publish");
        context.addServlet(new ServletHolder(subscribeServlet), "/subscribe");

        if (args.length > 0 && args[0] != null) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException unimportant) {
                //use standard port
            }
        }
        Server server = new Server(port);
        server.setHandler(context);
        server.start();
        logger.info("Server start on port: {}", port);
        System.out.println("Server start on port: " + port);

        server.join();

        timer.cancel();
        sendMessagesTimerTask.shutdown();
        backupTasks.shutdown();

    }
}

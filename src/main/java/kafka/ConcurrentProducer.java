package kafka;

import config.Conf;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentProducer {

    public static void main(String[] args) {
        File[] files = new File(Conf.DATA_PATH).listFiles();
        BlockingQueue<File> queue = new ArrayBlockingQueue<File>(240);
        for (File file : files) {
            queue.add(file);
        }

        ExecutorService pool = Executors.newFixedThreadPool(5);
        File currFile = null;
        while ((currFile = queue.poll()) != null) {
            System.out.println(currFile);
            Producer producer = new Producer(currFile);
            pool.execute(producer);
        }
        pool.shutdown();

    }
}

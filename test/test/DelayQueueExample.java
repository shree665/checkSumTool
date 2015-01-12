/**
 * 
 */
package test;

/**
 * @author vivek.subedi
 *
 */
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;

public class DelayQueueExample {
    public static void main(String[] args) {
        //
        // Creates an instance of blocking queue using the DelayQueue.
        //
        final BlockingQueue<DelayObject> queue = new DelayQueue<DelayObject>();
        final Random random = new Random();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //
                        // Put some Delayed object into the Queue.
                        //
                        int delay = random.nextInt(10000);
                        DelayObject object = new DelayObject(
                                UUID.randomUUID().toString(), delay);

                        System.out.printf("["+Thread.currentThread().getName()+"] Put object = %s%n", object);
                        queue.put(object);
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "Producer Thread").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //
                        // Take elements out from the DelayQueue object.
                        //
                        DelayObject object = queue.take();
                        System.out.printf("[%s] - Take object = %s%n",
                                Thread.currentThread().getName(), object);
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "Consumer Thread-1").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        //
                        // Take elements out from the DelayQueue object.
                        //
                        DelayObject object = queue.take();
                        System.out.printf("[%s] - Take object = %s%n",
                                Thread.currentThread().getName(), object);
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "Consumer Thread-2").start();
    }
}
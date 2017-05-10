package cn.memedai.orientdb.fraud.statistics.main;

import java.util.Date;
import java.util.LinkedList;

/**
 * Created by hangyu on 2017/5/5.
 */
public class TracingThreadPool extends ThreadGroup {
    private boolean isClosed = false;
    // 表示工作队列
    private LinkedList<Runnable> workQueue;
    // 表示线程池ID
    private static int threadPoolID;
    // 表示工作线程ID
    private int threadID;

    // poolSize指定线程池中的工作线程数目
    public TracingThreadPool(int poolSize) {
        super("ThreadPool-" + (threadPoolID++));
        setDaemon(true);
        // 创建工作队列
        workQueue = new LinkedList<Runnable>();
        for (int i = 0; i < poolSize; i++)
            // 创建并启动工作线程
            new WorkThread().start();
    }

    public synchronized void addTask(Runnable task) {
        // 线程池被关则抛出IllegalStateException异常
        if (isClosed) {
            throw new IllegalStateException();
        }
        if (task != null) {
            workQueue.add(task);
            // 唤醒正在getTask()方法中等待任务的工作线程
            notify();
        }
    }

    protected synchronized Runnable getTask() throws InterruptedException {
        while (workQueue.size() == 0) {
            if (isClosed)
                return null;
            // 如果工作队列中没有任务，就等待任务
            wait();
        }
        return workQueue.removeFirst();
    }

    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;
            workQueue.clear(); // 清空工作队列
            interrupt(); // 中断所有的工作线程，该方法继承自ThreadGroup类
        }
    }

    public void join() {
        synchronized (this) {
            isClosed = true;
            // 唤醒还在getTask()方法中等待任务的工作线程
            notifyAll();
        }
        Thread[] threads = new Thread[activeCount()];
        //获得线程组中当前所有活着的工作线程
        int count = enumerate(threads);
        // 等待所有工作线程运行结束
        for (int i = 0; i < count; i++) {
            try {
                // 等待工作线程运行结束
                threads[i].join();
            } catch (InterruptedException ex) {
            }
        }
    }

    public class WorkThread extends Thread {
        public WorkThread() {
            // 加入到当前ThreadPool线程组中
            super(TracingThreadPool.this, "WorkThread-" + (threadID++));
        }

        public void run() {
            while (!isInterrupted()) { // isInterrupted()方法继承自Thread类，判断线程是否被中断
                Runnable task = null;
                try {
                    // 得到任务
                    task = getTask();
                } catch (InterruptedException ex) {
                }
                // 如果getTask()返回null或者线程执行getTask()时被中断，则结束此线程
                if (task == null)
                    return;
                try {
                    // 运行任务，捕获异常
                    task.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }
}

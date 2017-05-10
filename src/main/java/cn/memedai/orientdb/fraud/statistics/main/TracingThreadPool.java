package cn.memedai.orientdb.fraud.statistics.main;

import java.util.Date;
import java.util.LinkedList;

/**
 * Created by hangyu on 2017/5/5.
 */
public class TracingThreadPool extends ThreadGroup {
    private boolean isClosed = false;
    // ��ʾ��������
    private LinkedList<Runnable> workQueue;
    // ��ʾ�̳߳�ID
    private static int threadPoolID;
    // ��ʾ�����߳�ID
    private int threadID;

    // poolSizeָ���̳߳��еĹ����߳���Ŀ
    public TracingThreadPool(int poolSize) {
        super("ThreadPool-" + (threadPoolID++));
        setDaemon(true);
        // ������������
        workQueue = new LinkedList<Runnable>();
        for (int i = 0; i < poolSize; i++)
            // ���������������߳�
            new WorkThread().start();
    }

    public synchronized void addTask(Runnable task) {
        // �̳߳ر������׳�IllegalStateException�쳣
        if (isClosed) {
            throw new IllegalStateException();
        }
        if (task != null) {
            workQueue.add(task);
            // ��������getTask()�����еȴ�����Ĺ����߳�
            notify();
        }
    }

    protected synchronized Runnable getTask() throws InterruptedException {
        while (workQueue.size() == 0) {
            if (isClosed)
                return null;
            // �������������û�����񣬾͵ȴ�����
            wait();
        }
        return workQueue.removeFirst();
    }

    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;
            workQueue.clear(); // ��չ�������
            interrupt(); // �ж����еĹ����̣߳��÷����̳���ThreadGroup��
        }
    }

    public void join() {
        synchronized (this) {
            isClosed = true;
            // ���ѻ���getTask()�����еȴ�����Ĺ����߳�
            notifyAll();
        }
        Thread[] threads = new Thread[activeCount()];
        //����߳����е�ǰ���л��ŵĹ����߳�
        int count = enumerate(threads);
        // �ȴ����й����߳����н���
        for (int i = 0; i < count; i++) {
            try {
                // �ȴ������߳����н���
                threads[i].join();
            } catch (InterruptedException ex) {
            }
        }
    }

    public class WorkThread extends Thread {
        public WorkThread() {
            // ���뵽��ǰThreadPool�߳�����
            super(TracingThreadPool.this, "WorkThread-" + (threadID++));
        }

        public void run() {
            while (!isInterrupted()) { // isInterrupted()�����̳���Thread�࣬�ж��߳��Ƿ��ж�
                Runnable task = null;
                try {
                    // �õ�����
                    task = getTask();
                } catch (InterruptedException ex) {
                }
                // ���getTask()����null�����߳�ִ��getTask()ʱ���жϣ���������߳�
                if (task == null)
                    return;
                try {
                    // �������񣬲����쳣
                    task.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }
}

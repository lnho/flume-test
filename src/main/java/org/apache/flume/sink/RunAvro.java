package org.apache.flume.sink;

import com.google.common.base.Charsets;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RunAvro {
    private static final Logger logger = LoggerFactory.getLogger(RunAvro.class);
    private final static Integer[] ports = {41411, 41412, 41413, 41414, 41415, 41416, 41417, 41418};
    int batchSize = 10000;
    long capacity = 10000L;
    private AvroSink sink;
    private Channel channel;
    private List<Event> logs = new ArrayList<>();

    public static void main(String[] args) {
        Executor executor = Executors.newCachedThreadPool();
        for (Integer port : ports) {
            executor.execute(() -> {
                RunAvro r = new RunAvro();
                try {
                    r.testProcess("10.206.19.189", port);
                } catch (InterruptedException | EventDeliveryException | InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void setUp(String hostname, Integer port) {
        setUp("none", 0, hostname, port);
    }

    public void setUp(String compressionType, int compressionLevel, String hostname, Integer port) {
        if (sink != null) {
            throw new RuntimeException("double setup");
        }
        sink = new AvroSink();
        channel = new MemoryChannel();

        Context context = new Context();

        context.put("hostname", hostname);
        context.put("port", String.valueOf(port));

        context.put("batch-size", String.valueOf(batchSize));
        context.put("connect-timeout", String.valueOf(10000L));
        context.put("request-timeout", String.valueOf(10000L));

        context.put("capacity", String.valueOf(capacity));
        context.put("transactionCapacity", String.valueOf(capacity));
        if (compressionType.equals("deflate")) {
            context.put("compression-type", compressionType);
            context.put("compression-level", Integer.toString(compressionLevel));
        }

        sink.setChannel(channel);

        Configurables.configure(sink, context);
        Configurables.configure(channel, context);
        List<String> strings = FileUtil.readTxtFileIntoStringArrList(ClassLoader.getSystemResource("1k.log").getPath());

        strings.forEach(str -> {
            Event event = EventBuilder.withBody(str, Charsets.UTF_8);
            logs.add(event);
        });
        System.out.println("已加载" + logs.size() + "行日志");
    }

    private void testProcess(String hostname, Integer port) throws InterruptedException,
            EventDeliveryException, InstantiationException, IllegalAccessException {
        setUp(hostname, port);

        sink.start();

        for (; ; ) {
            Transaction transaction = channel.getTransaction();

            transaction.begin();
            for (int i = 0; i < batchSize; i++) {
                Event event = logs.get(i % logs.size());
                channel.put(event);
            }
            transaction.commit();
            transaction.close();
            sink.process();
        }

//        sink.stop();
    }

}

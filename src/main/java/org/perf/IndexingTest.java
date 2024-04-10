package org.perf;

import org.HdrHistogram.Histogram;
import org.cache.impl.DummyCacheFactory;
import org.cache.impl.HazelcastCacheFactory;
import org.cache.impl.InfinispanCacheFactory;
import org.cache.impl.tri.TriCacheFactory;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.*;
import org.perf.invokers.CommitInvoker;
import org.perf.invokers.Invoker;
import org.perf.invokers.ReserveInvoker;
import org.perf.model.Account;
import org.perf.model.AccountActionInitializer;
import org.perf.model.UETRActionInitializer;

import javax.management.MBeanServer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;


/**
 * Mimics distributed mode by invoking N (default:50000) requests, 20% writes and 80% reads on random keys in range
 * [1 .. N]. Every member in the cluster does this and the initiator waits until everyone is done, tallies the results
 * (sent to it by every member) and prints stats (throughput).
 *
 * @author Bela Ban
 */
public class IndexingTest implements Receiver {

    public static final String ACCOUNT_CACHE = "AccountP2";
    public static final String ACTION_CACHE = "UETRActionP2";

    protected RemoteCacheManager remoteCacheManager;
    protected JChannel control_channel;
    protected Address local_addr;
    protected final List<Address> members = new ArrayList<>();
    protected volatile View view;
    protected volatile boolean looping = true;
    protected final ResponseCollector<Results> results = new ResponseCollector<>();
    protected final Promise<Config> config_promise = new Promise<>();
    protected Thread test_runner;
    protected ThreadFactory thread_factory;

    protected enum Type {
        START_ISPN,
        GET_CONFIG_REQ,
        GET_CONFIG_RSP,       // Config
        SET,                  // field-name (String), value (Object)
        QUIT_ALL,
        RESULTS               // Results
    }

    // ============ configurable properties ==================
    @Property
    protected int num_threads = 1;
    @Property
    protected int time_secs = 60, msg_size = 1000;
    @Property
    protected double read_percentage = 0.8; // 80% reads, 20% writes
    @Property
    protected boolean print_details = true;
    @Property
    protected boolean print_invokers;
    @Property
    protected String accountId = "1234";
    // ... add your own here, just don't forget to annotate them with @Property
    // =======================================================

    public static final double[] PERCENTILES = {50, 90, 95, 99, 99.9};

    protected static final String infinispan_factory = InfinispanCacheFactory.class.getName();
    protected static final String hazelcast_factory = HazelcastCacheFactory.class.getName();
    protected static final String coherence_factory = "org.cache.impl.CoherenceCacheFactory"; // to prevent loading of Coherence up-front
    protected static final String tri_factory = TriCacheFactory.class.getName();
    protected static final String dummy_factory = DummyCacheFactory.class.getName();

    protected static final String input_str = "[1] Start test [2] View [3] Cache size [4] Threads (%d) " +
            "\n[6] Time (secs) (%d) [7] Value size (%s) [8] Validate" +
            "\n[p] Populate cache [c] Clear cache [v] Versions" +
            "\n[r] Read percentage (%.2f) " +
            "\n[d] Details (%b)  [i] Invokers (%b) [l] dump local cache" +
            "\n[s] Register Schema" +
            "\n[q] Quit [X] Quit all\n";

    static {
        ClassConfigurator.add((short) 11000, Results.class);
    }

    public void init(String cfg, String jgroups_config, boolean use_virtual_threads) throws Exception {
        thread_factory = new DefaultThreadFactory("invoker", false, true)
                .useVirtualThreads(use_virtual_threads);
        if (use_virtual_threads && Util.virtualThreadsAvailable())
            System.out.println("-- using virtual threads");

        Properties properties = new Properties();
        properties.load(FileLookupFactory.newInstance().lookupFile(cfg, getClass().getClassLoader()));
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.withProperties(properties);

        builder.remoteCache(ACCOUNT_CACHE)
                .transactionMode(TransactionMode.NON_DURABLE_XA)
                .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());
        builder.remoteCache(ACTION_CACHE)
                .transactionMode(TransactionMode.NON_DURABLE_XA)
                .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());

        builder.addContextInitializer(AccountActionInitializer.INSTANCE);
        builder.addContextInitializer(UETRActionInitializer.INSTANCE);


        remoteCacheManager = new RemoteCacheManager(builder.build());
        remoteCacheManager.getCache(ACCOUNT_CACHE);
        control_channel = new JChannel(jgroups_config);
        control_channel.setReceiver(this);
        control_channel.connect("cfg");
        local_addr = control_channel.getAddress();

        try {
            MBeanServer server = Util.getMBeanServer();
            JmxConfigurator.registerChannel(control_channel, server, "control-channel", control_channel.getClusterName(), true);
        } catch (Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }

        if (members.size() >= 2) {
            Address coord = members.get(0);
            config_promise.reset(true);
            send(coord, Type.GET_CONFIG_REQ);
            Config config = config_promise.getResult(5000);
            if (config != null) {
                applyConfig(config);
                System.out.println("Fetched config from " + coord + ": " + config);
            } else
                System.err.println("failed to fetch config from " + coord);
        }
    }

    void stop() {
        Util.close(control_channel);
        remoteCacheManager.stop();
    }


    protected synchronized void startTestRunner(final Address addr) {
        if (test_runner != null && test_runner.isAlive())
            System.err.println("test is already running - wait until complete to start a new run");
        else {
            test_runner = new Thread(() -> startIspnTest(addr), "testrunner");
            test_runner.start();
        }
    }

    public void receive(Message msg) {
        try {
            _receive(msg);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


    protected void _receive(Message msg) throws Throwable {
        ByteArrayDataInputStream in;
        Address sender = msg.src();
        int offset = msg.getOffset(), len = msg.getLength();
        byte[] buf = msg.getArray();
        byte t = buf[offset++];
        len--;
        Type type = Type.values()[t];
        switch (type) {
            case START_ISPN:
                startTestRunner(sender);
                break;
            case GET_CONFIG_REQ:
                send(sender, Type.GET_CONFIG_RSP, getConfig());
                break;
            case GET_CONFIG_RSP:
                in = new ByteArrayDataInputStream(buf, offset, len);
                Config cfg = Util.objectFromStream(in);
                config_promise.setResult(cfg);
                break;
            case SET:
                in = new ByteArrayDataInputStream(buf, offset, len);
                String field_name = Util.objectFromStream(in);
                Object val = Util.objectFromStream(in);
                set(field_name, val);
                break;
            case QUIT_ALL:
                quitAll();
                break;
            case RESULTS:
                in = new ByteArrayDataInputStream(buf, offset, len);
                Results res = Util.objectFromStream(in);
                results.add(sender, res);
                break;
            default:
                throw new IllegalArgumentException(String.format("type %s not known", type));
        }
    }

    public void viewAccepted(View new_view) {
        this.view = new_view;
        members.clear();
        members.addAll(new_view.getMembers());
    }


    // =================================== callbacks ======================================


    protected void startIspnTest(Address sender) {
        try {
            System.out.printf("Running test for %d seconds:\n", time_secs);

            // The first call needs to be synchronous with OOB !
            final CountDownLatch latch = new CountDownLatch(1);
            ReserveInvoker[] reserve = new ReserveInvoker[num_threads];
            Invoker[] commit = new Invoker[num_threads];
            List<Thread> threads = new ArrayList<>(num_threads * 2);
            for (int i = 0; i < num_threads; i++) {
                reserve[i] = new ReserveInvoker(latch, remoteCacheManager);
                commit[i] = new CommitInvoker(latch, remoteCacheManager);
                var t = thread_factory.newThread(reserve[i]);
                t.setName("reserve-" + i);
                t.start();
                threads.add(t);

                t = thread_factory.newThread(commit[i]);
                t.setName("commit-" + i);
                t.start();
                threads.add(t);
            }

            double tmp_interval = (time_secs * 1000.0) / 10.0;
            long interval = (long) tmp_interval;

            long start = System.currentTimeMillis();
            latch.countDown(); // starts all sender threads
            for (int i = 1; i <= 10; i++) {
                Util.sleep(interval);
                System.out.print(".");
            }

            Arrays.stream(reserve).forEach(Invoker::cancel);
            Arrays.stream(commit).forEach(Invoker::cancel);
            for (Thread t : threads)
                t.join();

            long time = System.currentTimeMillis() - start;
            System.out.println("\ndone (in " + time + " ms)\n");

            Histogram reserveHist = null;
            Histogram commitHist = null;
            Histogram queryHist = null;

            long reserveRequests = 0;
            long commitRequests = 0;

            for (int i = 0; i < num_threads; ++i) {
                if (reserveHist == null) {
                    reserveHist = reserve[i].getResults();
                } else {
                    reserveHist.add(reserve[i].getResults());
                }
                if (queryHist == null) {
                    queryHist = reserve[i].getQuery();
                } else {
                    queryHist.add(reserve[i].getQuery());
                }
                if (commitHist == null) {
                    commitHist = commit[i].getResults();
                } else {
                    commitHist.add(commit[i].getResults());
                }

                reserveRequests += reserve[i].getCount();
                commitRequests += commit[i].getCount();
            }

            Results result = new Results(commitHist, commitRequests, reserveHist, reserveRequests, queryHist, time);
            send(sender, Type.RESULTS, result);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected void quitAll() {
        System.out.println("-- received quitAll(): shutting down");
        looping = false;
        stop();
        System.exit(0);
    }


    protected void set(String field_name, Object value) {
        Field field = Util.getField(this.getClass(), field_name);
        if (field == null)
            System.err.println("Field " + field_name + " not found");
        else {
            Util.setField(field, this, value);
            System.out.println(field.getName() + "=" + value);
        }
    }


    protected Config getConfig() {
        Config config = new Config();
        for (Field field : Util.getAllDeclaredFieldsWithAnnotations(IndexingTest.class, Property.class))
            config.add(field.getName(), Util.getField(field, this));
        return config;
    }

    protected void applyConfig(Config config) {
        for (Map.Entry<String, Object> entry : config.values.entrySet()) {
            Field field = Util.getField(getClass(), entry.getKey());
            Util.setField(field, this, entry.getValue());
        }
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() throws Throwable {
        while (looping) {
            int c = Util.keyPress(String.format(input_str,
                    num_threads, time_secs, Util.printBytes(msg_size),
                    read_percentage, print_details, print_invokers));
            switch (c) {
                case '1':
                    startBenchmark();
                    break;
                case '2':
                    printView();
                    break;
                case '3':
                    printCacheSize();
                    break;
                case '4':
                    changeFieldAcrossCluster("num_threads", Util.readIntFromStdin("Number of sender threads: "));
                    break;
                case '5':
                    changeFieldAcrossCluster("num_keys", Util.readIntFromStdin("Number of keys: "));
                    break;
                case '6':
                    changeFieldAcrossCluster("time_secs", Util.readIntFromStdin("Time (secs): "));
                    break;
                case '7':
                    int new_msg_size = Util.readIntFromStdin("Message size: ");
                    if (new_msg_size < Global.LONG_SIZE * 3)
                        System.err.println("msg size must be >= " + Global.LONG_SIZE * 3);
                    else
                        changeFieldAcrossCluster("msg_size", new_msg_size);
                    break;
                case 'c':
                    clearCache();
                    break;
                case 'd':
                    changeFieldAcrossCluster("print_details", !print_details);
                    break;
                case 'i':
                    changeFieldAcrossCluster("print_invokers", !print_invokers);
                    break;
                case 'r':
                    double percentage = getReadPercentage();
                    if (percentage >= 0)
                        changeFieldAcrossCluster("read_percentage", percentage);
                    break;
                case 'p':
                    populateCache();
                    break;
                case 'v':
                    System.out.printf("JGroups: %s, Infinispan: %s\n",
                            Version.printDescription(),
                            org.infinispan.commons.util.Version.printVersion());
                    break;
                case 'q':
                case 0: // remove on upgrade to next JGroups version
                case -1:
                    stop();
                    return;
                case 's':
                    remoteCacheManager.getCache("___protobuf_metadata").put(AccountActionInitializer.INSTANCE.getProtoFileName(), AccountActionInitializer.INSTANCE.getProtoFile());
                    remoteCacheManager.getCache("___protobuf_metadata").put(UETRActionInitializer.INSTANCE.getProtoFileName(), UETRActionInitializer.INSTANCE.getProtoFile());
                    break;
                case 'X':
                    try {
                        control_channel.send(null, new byte[]{(byte) Type.QUIT_ALL.ordinal()});
                    } catch (Throwable t) {
                        System.err.println("Calling quitAll() failed: " + t);
                    }
                    break;
                default:
                    break;
            }
        }
    }


    /**
     * Kicks off the benchmark on all cluster nodes
     */
    protected void startBenchmark() {
        results.reset(members);
        try {
            send(null, Type.START_ISPN);
        } catch (Throwable t) {
            System.err.printf("starting the benchmark failed: %s\n", t);
            return;
        }

        // wait for time_secs seconds pus 20%
        boolean all_results = results.waitForAllResponses((long) (time_secs * 1000 * 1.2));
        if (!all_results)
            System.err.printf("did not receive all results: missing results from %s\n", results.getMissing());

        long totalReserve = 0, totalCommit = 0, total_time = 0, longest_time = 0;
        Histogram reserveHist = null;
        Histogram commitHist = null;
        Histogram queryHist = null;

        System.out.println("\n======================= Results: ===========================");
        for (Map.Entry<Address, Results> entry : results.getResults().entrySet()) {
            Address mbr = entry.getKey();
            Results result = entry.getValue();
            if (result != null) {
                totalReserve += result.reserveRequests;
                totalCommit += result.commitRequests;
                total_time += result.time;
                longest_time = Math.max(longest_time, result.time);
                if (reserveHist == null) {
                    reserveHist = result.reserveHist;
                } else {
                    reserveHist.add(result.reserveHist);
                }
                if (queryHist == null) {
                    queryHist = result.queryHist;
                } else {
                    queryHist.add(result.queryHist);
                }
                if (commitHist == null) {
                    commitHist = result.commitHist;
                } else {
                    commitHist.add(result.commitHist);
                }
            }
            System.out.println(mbr + ": " + result);
        }
        double reqs_sec_node = (totalReserve + totalCommit) / (total_time / 1000.0);
        double reqs_sec_cluster = (totalReserve + totalCommit) / (longest_time / 1000.0);
        System.out.println("\n");
        System.out.println(Util.bold(String.format("Throughput: %,.0f reqs/sec/node %,.0f reqs/sec/cluster\n" +
                        "Reserve: \n" +
                        "\tQuery: %s\n" +
                        "\tTotal: %s\n" +
                        "Commit:  %s",
                reqs_sec_node, reqs_sec_cluster, print(queryHist, print_details), print(reserveHist, print_details), print(commitHist, print_details))));
        System.out.println("\n\n");
    }


    protected static double getReadPercentage() throws Exception {
        double tmp = Util.readDoubleFromStdin("Read percentage: ");
        if (tmp < 0 || tmp > 1.0) {
            System.err.println("read percentage must be >= 0 or <= 1.0");
            return -1;
        }
        return tmp;
    }


    protected void changeFieldAcrossCluster(String field_name, Object value) throws Exception {
        send(null, Type.SET, field_name, value);
    }


    protected void send(Address dest, Type type, Object... args) throws Exception {
        if (args == null || args.length == 0) {
            control_channel.send(dest, new byte[]{(byte) type.ordinal()});
            return;
        }
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(512);
        out.write((byte) type.ordinal());
        for (Object arg : args)
            Util.objectToStream(arg, out);
        control_channel.send(dest, out.buffer(), 0, out.position());
    }


    protected void printView() {
        System.out.printf("\n-- local: %s\n-- view: %s\n", local_addr, view);
        try {
            System.in.skip(System.in.available());
        } catch (Exception e) {
        }
    }

    protected static String print(Histogram avg, boolean details) {
        if (avg == null || avg.getTotalCount() == 0)
            return "n/a";
        return details ? String.format("min/avg/max = %d/%,.2f/%,.2f us (%s)",
                avg.getMinValue(), avg.getMean(), avg.getMaxValueAsDouble(), percentiles(avg)) :
                String.format("avg = %,.2f us", avg.getMean());
    }

    protected static String percentiles(Histogram h) {
        StringBuilder sb = new StringBuilder();
        for (double percentile : PERCENTILES) {
            long val = h.getValueAtPercentile(percentile);
            sb.append(String.format("%,.1f=%,d ", percentile, val));
        }
        sb.append(String.format("[percentile at mean: %,.2f]", h.getPercentileAtOrBelowValue((long) h.getMean())));
        return sb.toString();
    }

    protected void printCacheSize() {
        Stream.of(ACCOUNT_CACHE, ACTION_CACHE)
                .map(remoteCacheManager::getCache)
                .forEach(c -> System.out.println("-- cache '" + c.getName() + "' has " + c.size() + " elements"));
    }


    protected void clearCache() {
        Stream.of(ACCOUNT_CACHE, ACTION_CACHE)
                .map(remoteCacheManager::getCache)
                .forEach(RemoteCache::clear);
    }

    // Inserts num_keys keys into the cache (in parallel)
    protected void populateCache() {
        Account account = new Account(accountId, "0", "0", "0");
        remoteCacheManager.getCache(ACCOUNT_CACHE).put(accountId, account);
    }

    protected static void writeTo(UUID addr, long seqno, byte[] buf, int offset) {
        long low = addr.getLeastSignificantBits(), high = addr.getMostSignificantBits();
        Bits.writeLong(low, buf, offset);
        Bits.writeLong(high, buf, offset + Global.LONG_SIZE);
        Bits.writeLong(seqno, buf, offset + Global.LONG_SIZE * 2);
    }


    public static class Results implements Streamable {
        protected long reserveRequests, commitRequests, time; // ms
        protected Histogram reserveHist, commitHist, queryHist;

        public Results() {
        }

        public Results(Histogram commitHist, long commitRequests, Histogram reserveHist, long reserveRequests, Histogram queryHist, long time) {
            this.commitHist = commitHist;
            this.commitRequests = commitRequests;
            this.reserveHist = reserveHist;
            this.reserveRequests = reserveRequests;
            this.queryHist = queryHist;
            this.time = time;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(reserveRequests);
            out.writeLong(commitRequests);
            out.writeLong(time);
            write(reserveHist, out);
            write(commitHist, out);
            write(queryHist, out);
        }

        public void readFrom(DataInput in) throws IOException {
            reserveRequests = in.readLong();
            commitRequests = in.readLong();
            time = in.readLong();
            try {
                reserveHist = read(in);
                commitHist = read(in);
                queryHist = read(in);
            } catch (DataFormatException dfex) {
                throw new IOException(dfex);
            }
        }

        public String toString() {
            double rps = reserveRequests / (time / 1000.0);
            double cps = commitRequests / (time / 1000.0);
            return String.format("Reserve: %,.2f reqs/sec (%,d requests), avg RTT (us) = %,.2f\n" +
                            "Commit:  %,.2f reqs/sec (%,d requests), avg RTT (us) = %,.2f",
                    rps, reserveRequests, reserveHist.getMean(), cps, commitRequests, commitHist.getMean());
        }
    }

    protected static void write(Histogram h, DataOutput out) throws IOException {
        int size = h.getEstimatedFootprintInBytes();
        ByteBuffer buf = ByteBuffer.allocate(size);
        h.encodeIntoCompressedByteBuffer(buf, 9);
        out.writeInt(buf.position());
        out.write(buf.array(), 0, buf.position());
    }

    protected static Histogram read(DataInput in) throws IOException, DataFormatException {
        int len = in.readInt();
        byte[] array = new byte[len];
        in.readFully(array);
        ByteBuffer buf = ByteBuffer.wrap(array);
        return Histogram.decodeFromCompressedByteBuffer(buf, 0);
    }


    public static class Config implements Streamable {
        protected final Map<String, Object> values = new HashMap<>();

        public Config() {
        }

        public Config add(String key, Object value) {
            values.put(key, value);
            return this;
        }

        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(values.size());
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                Bits.writeString(entry.getKey(), out);
                Util.objectToStream(entry.getValue(), out);
            }
        }

        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = Bits.readString(in);
                Object value = Util.objectFromStream(in);
                if (key == null)
                    continue;
                values.put(key, value);
            }
        }

        public String toString() {
            return values.toString();
        }
    }


    public static void main(String[] args) {
        String config_file = "dist-sync.xml";
        String cache_name = "perf-cache";
        String cache_factory_name = InfinispanCacheFactory.class.getName();
        String control_cfg = "control.xml";
        boolean run_event_loop = true;
        boolean use_vthreads = true;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cfg")) {
                config_file = args[++i];
                continue;
            }
            if (args[i].equals("-cache")) {
                cache_name = args[++i];
                continue;
            }
            if ("-factory".equals(args[i])) {
                cache_factory_name = args[++i];
                continue;
            }
            if ("-nohup".equals(args[i])) {
                run_event_loop = false;
                continue;
            }
            if ("-control-cfg".equals(args[i])) {
                control_cfg = args[++i];
                continue;
            }
            if ("-use-virtual-threads".equals(args[i]) || "-use-vthreads".equals(args[i])) {
                use_vthreads = Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        IndexingTest test = null;
        try {
            test = new IndexingTest();
            switch (cache_factory_name) {
                case "ispn":
                    cache_factory_name = infinispan_factory;
                    break;
                case "hc":
                    cache_factory_name = hazelcast_factory;
                    break;
                case "coh":
                    cache_factory_name = coherence_factory;
                    break;
                case "tri":
                    cache_factory_name = tri_factory;
                    break;
                case "dummy":
                    cache_factory_name = dummy_factory;
                    break;
            }
            test.init(config_file, control_cfg, use_vthreads);
            Runtime.getRuntime().addShutdownHook(new Thread(test::stop));
            if (run_event_loop)
                test.eventLoop();
        } catch (Throwable ex) {
            ex.printStackTrace();
            if (test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.printf("Test [-factory <cache factory classname>] [-cfg <config-file>] " +
                        "[-cache <cache-name>] [-control-cfg] [-nohup]\n" +
                        "Valid factory names:" +
                        "\n  ispn: %s\n  hc:   %s\n  coh:  %s\n  tri:  %s\n dummy: %s\n\n",
                infinispan_factory, hazelcast_factory, coherence_factory, tri_factory, dummy_factory);
    }


}
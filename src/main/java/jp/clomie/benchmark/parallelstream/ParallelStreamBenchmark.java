package jp.clomie.benchmark.parallelstream;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by clomie on 14/02/23.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class ParallelStreamBenchmark {

    private static final Map<String, Function<Integer, Collection<Integer>>> collectionMap;
    private static final Map<String, Function<Collection<Integer>, Object>> functionMap;

    static {

        Function<Integer, Collection<Integer>> create =
                i -> {
                    List<Integer> list = new LinkedList<>();
                    IntStream.range(0, i).forEach(list::add);
                    return list;
                };

        Map<String, Function<Integer, Collection<Integer>>> cmap = new HashMap<>();
        cmap.put("LinkedList", i -> new LinkedList<>(create.apply(i)));
        cmap.put("ArrayList", i -> new ArrayList<>(create.apply(i)));
        cmap.put("CopyOnWriteArrayList", i -> new CopyOnWriteArrayList<>(create.apply(i)));
        cmap.put("HashSet", i -> new HashSet<>(create.apply(i)));
        cmap.put("LinkedHashSet", i -> new LinkedHashSet<>(create.apply(i)));
        cmap.put("TreeSet", i -> new TreeSet<>(create.apply(i)));
        cmap.put("ConcurrentSkipListSet", i -> new CopyOnWriteArraySet<>(create.apply(i)));
        cmap.put("CopyOnWriteArraySet", i -> new ConcurrentSkipListSet<>(create.apply(i)));
        collectionMap = Collections.unmodifiableMap(cmap);

        Function<Stream<Integer>, Double> benchmark1 =
                s -> s.collect(Collectors.averagingInt(i -> i));

        Function<Stream<Integer>, String> benchmark2 =
                s -> s.map(Object::toString).collect(Collectors.joining(","));

        Map<String, Function<Collection<Integer>, Object>> fmap = new HashMap<>();
        fmap.put("1:IntegerSerial", s -> benchmark1.apply(s.stream()));
        fmap.put("2:IntegerParallel", s -> benchmark1.apply(s.parallelStream()));
        fmap.put("3:StringSerial", s -> benchmark2.apply(s.stream()));
        fmap.put("4:StringParallel", s -> benchmark2.apply(s.parallelStream()));
        functionMap = Collections.unmodifiableMap(fmap);
    }

    public static void main(String[] args) throws IOException, RunnerException {

        Main.main(args);
    }

    @Param({"LinkedList", "ArrayList", "CopyOnWriteArrayList", "HashSet", "LinkedHashSet", "TreeSet", "ConcurrentSkipListSet", "CopyOnWriteArraySet"})
    public String param1CollectionClass;

    @Param({"1000", "10000", "100000"})
    public String param2Size;

    @Param({"1:IntegerSerial", "2:IntegerParallel", "3:StringSerial", "4:StringParallel"})
    public String param3StreamType;

    private Collection<Integer> c;
    private Function<Collection<Integer>, Object> f;
    private Object r;

    @Setup
    public void setup() {
        System.out.println("########## SETUP ##########");
        c = collectionMap.get(param1CollectionClass).apply(Integer.parseInt(param2Size));
        f = functionMap.get(param3StreamType);
    }

    @GenerateMicroBenchmark
    public Object test1Collections() {
        return f.apply(c);
    }

    @TearDown
    public void tearDown() {
        c.clear();
        System.out.println(" ########## TEAR DOWN ##########");
    }
}

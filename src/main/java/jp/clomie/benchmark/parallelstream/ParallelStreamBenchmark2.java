package jp.clomie.benchmark.parallelstream;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by clomie on 14/02/27.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class ParallelStreamBenchmark2 {


    private static final Map<String, Function<Integer, Stream<Integer>>> integerStreamMap;
    private static final Map<String, Function<Integer, IntStream>> intStreamMap;

    private static final Map<String, Function<Stream<Integer>, Object>> boxedFuncMap;
    private static final Map<String, Function<IntStream, Object>> intFuncMap;

    static {
        Map<String, Function<Integer, Stream<Integer>>> bmap = new HashMap<>();
        bmap.put("range", i -> IntStream.range(0, i).boxed());
        bmap.put("array", i -> Arrays.stream(IntStream.range(0, i).toArray()).boxed());
        bmap.put("streamOf", i -> Stream.of(IntStream.range(0, i).boxed().toArray(Integer[]::new)));
        bmap.put("iterate", i -> Stream.iterate(0, e -> ++e).limit(i));
        integerStreamMap = bmap;

        Map<String, Function<Integer, IntStream>> imap = new HashMap<>();
        imap.put("range", i -> IntStream.range(0, i));
        imap.put("array", i -> Arrays.stream(IntStream.range(0, i).toArray()));
        imap.put("streamOf", i -> IntStream.of(IntStream.range(0, i).toArray()));
        imap.put("iterate", i -> IntStream.iterate(0, e -> e++).limit(i));
        intStreamMap = imap;

        Function<Stream<Integer>, Double> benchmark1 = s -> s.collect(Collectors.averagingInt(i -> i));
        Function<Stream<Integer>, String> benchmark2 = s -> s.map(Object::toString).collect(Collectors.joining(","));
        Map<String, Function<Stream<Integer>, Object>> fbmap = new HashMap<>();
        fbmap.put("1:IntegerSerial", benchmark1::apply);
        fbmap.put("2:IntegerParallel", s -> benchmark1.apply(s.parallel()));
        fbmap.put("3:StringSerial", benchmark2::apply);
        fbmap.put("4:StringParallel", s -> benchmark2.apply(s.parallel()));
        boxedFuncMap = Collections.unmodifiableMap(fbmap);

        Function<IntStream, Double> benchmarkInt1 = s -> s.average().orElse(0);
        Function<IntStream, String> benchmarkInt2 = s -> s.mapToObj(i -> Integer.toString(i)).collect(Collectors.joining(","));
        Map<String, Function<IntStream, Object>> fimap = new HashMap<>();
        fimap.put("1:IntegerSerial", benchmarkInt1::apply);
        fimap.put("2:IntegerParallel", s -> benchmarkInt1.apply(s.parallel()));
        fimap.put("3:StringSerial", benchmarkInt2::apply);
        fimap.put("4:StringParallel", s -> benchmarkInt2.apply(s.parallel()));
        intFuncMap = Collections.unmodifiableMap(fimap);
    }

    public static void main(String[] args) throws IOException, RunnerException {

        Main.main(args);
    }


    @Param({"range", "array", "streamOf", "iterate"})
    public String param1StreamType;

    @Param({"1000", "10000", "100000"})
    public String param2size;

    @Param({"1:IntegerSerial", "2:IntegerParallel", "3:StringSerial", "4:StringParallel"})
    public String param3FunctionType;

    private Stream<Integer> bs;

    private IntStream is;

    @Setup(Level.Invocation)
    public void createStream() {
        int size = Integer.parseInt(param2size);
        bs = integerStreamMap.get(param1StreamType).apply(size);
        is = intStreamMap.get(param1StreamType).apply(size);
    }

    @GenerateMicroBenchmark
    public void test2BoxedStream() {
        boxedFuncMap.get(param3FunctionType).apply(bs);
    }

    @GenerateMicroBenchmark
    public void test3PrimitiveStream() {
        intFuncMap.get(param3FunctionType).apply(is);
    }
}

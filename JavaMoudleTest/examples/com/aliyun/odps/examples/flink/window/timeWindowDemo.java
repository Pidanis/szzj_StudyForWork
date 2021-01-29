package com.aliyun.odps.examples.flink.window;

import com.aliyun.odps.examples.flink.beans.sensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

public class timeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\program_file\\IdeaProjects\\MaxComputerTest\\JavaMoudleTest\\examples\\com\\aliyun\\odps\\examples\\flink\\sensor.txt");

        // 转换为sensorReading类型
        SingleOutputStreamOperator<sensorReading> mapStream = dataStream.map(new MapFunction<String, sensorReading>() {

            @Override
            public sensorReading map(String s) throws Exception {
                String[] fields = s.split(", ");
                return new sensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });


        dataStream.timeWindowAll(Time.seconds(10), Time.seconds(1)).max("id");

        //窗口测试


//        《窗口分配器》window
//        滚动时间窗口（tumbling time window） .timeWindow(Time.seconds(15))
//        滑动时间窗口（sliding time window） .timeWindow(Time.seconds(15), Time.seconds(5))
//        会话窗口（session window） .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
//        滚动计数窗口（tumbling count window） .countWindow(15)
//        滑动计数窗口（sliding count window） .countWindow(10, 2)

//        《窗口函数》window function
//        增量聚合函数（incremental aggregation functions）
//        - 每条数据到来就进行计算，保持一个简单的状态
//        - ReduceFunction, AggregateFunction
//        全窗口函数（full window functions）
//        - 先把窗口所有数据收集起来，等到计算的时候会遍历所有数据
//        - ProcessWindowsFunction, WindowFunction





//        AllWindowedStream<String, Window> windowAllStream = dataStream.windowAll(); //所有元素放在一个操作实例，无并行
//        dataStream.keyBy("id").window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
//        dataStream.keyBy("id").timeWindow(Time.seconds(15)) //传参：一个 / 两个 --> 一个：就是上面Tumbling方法 / 两个：就是Sliading方法
//        dataStream.keyBy("id").window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//        dataStream.keyBy("id").window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)));

//        dataStream.keyBy("id").countWindow(10, 2);//传参：一个 / 两个 --> 一个：就是滚动计算窗口方法 / 两个：就是滑动计数窗口方法

        //增量聚合函数
        SingleOutputStreamOperator<Integer> windowStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<sensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() { // 累加器初始值
                        return 0;
                    }

                    @Override
                    public Integer add(sensorReading sensorReading, Integer integer) { // 累加方法
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) { // 输出结果
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) { // 状态合并
                        return integer + acc1;
                    }
                });

        //全量聚合函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> windowStream1 = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<sensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<sensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long window = timeWindow.getEnd();
                        int count = IteratorUtils.toList(input.iterator()).size();

                        out.collect(new Tuple3<>(id, window, count));
                    }
                });

        windowStream.print();
        windowStream1.print();

        /**
         * Time Window Test
         * **/
        AllWindowedStream<sensorReading, GlobalWindow> windowAllStream = mapStream.windowAll(GlobalWindows.create());
        windowAllStream.trigger(new Trigger<sensorReading, GlobalWindow>() {
            /**
             * CONTINUE：什么都不做。
             * FIRE：启动计算并将结果发送给下游，不清理窗口数据。
             * PURGE：清理窗口数据但不执行计算。
             * FIRE_AND_PURGE：启动计算，发送结果然后清理窗口数据。
             * **/
            @Override
            public TriggerResult onElement(sensorReading sensorReading, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

            }
        }).evictor(new Evictor<sensorReading, GlobalWindow>() {
            @Override
            public void evictBefore(Iterable<TimestampedValue<sensorReading>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

            }

            @Override
            public void evictAfter(Iterable<TimestampedValue<sensorReading>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

            }
        });

        env.execute();
    }
}

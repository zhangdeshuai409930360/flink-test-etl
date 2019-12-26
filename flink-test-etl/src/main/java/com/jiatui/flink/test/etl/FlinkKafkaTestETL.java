package com.jiatui.flink.test.etl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Properties;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
//import com.jiatui.flink.kafka.consumer.test.etl.FlinkJavaWordCount.WordWithcount;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

/**
 * kafkaSource
 *
 * 
 */
public class FlinkKafkaTestETL {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);
		// //checkpoint配置
		// // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
		// env.enableCheckpointing(60000);
		// // 高级选项：
		// // 设置模式为exactly-once （这是默认值）
		// env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
		// env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
		// // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
		// env.getCheckpointConfig().setCheckpointTimeout(10000);
		// // 同一时间只允许进行一个检查点
		// env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
		// env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		// //设置statebackend
		// env.setStateBackend(new
		// RocksDBStateBackend("hdfs://192.168.3.122:9000/flink/checkpoints",true));

		// flink重试命令 flink run -s
		// hdfs://192.168.103.4:8020/flink/checkpoints/6e5470589cd497b81e7b3719382faf0a/chk-2/_metadata
		// -m yarn-cluster -yn 2 -ytm 2048 -yjm 2048 -c
		// com.jiatui.flink.test.etl.FlinkKafkaTestETL
		// flink-test-etl-0.0.1-SNAPSHOT-jar-with-dependencies.jar

		env.enableCheckpointing(60000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
		env.getCheckpointConfig().setCheckpointTimeout(10000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// 设置statebackend
		// state.checkpoints.num-retained: 20（conf/flink-conf.yaml中）保存多份checkpoint文件
		// env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.103.4:8020/flink/checkpoints",true));

		// 重启次数，及其间隔
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10));
		// 设置使用EventTime时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String topic = "demo";
		Properties prop = new Properties();
		prop.setProperty("bootstrap.servers", "192.168.3.122:9092");
		prop.setProperty("group.id", "con1");

		FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);

		// 默认消费策略,重新运行默认读取上次的offset.
		myConsumer.setStartFromGroupOffsets();

		DataStreamSource<String> text = env.addSource(myConsumer);

		// DataStream<String> alldata = text.flatMap(new FlatMapFunction<String,
		// String>() {
		//
		// /**
		// *
		// */
		// private static final long serialVersionUID = 1L;
		//
		// public void flatMap(String value, Collector<String> out) throws Exception {
		// // TODO Auto-generated method stub
		// JSONObject jsonObject = JSONObject.parseObject(value);
		// String date = jsonObject.getString("dt");
		// Long timestape = jsonObject.getLong("CurrentTimeTemp");
		// String dt=date.replace(" ", "");
		// String countryCode = jsonObject.getString("countryCode");
		// JSONArray jsonArray = jsonObject.getJSONArray("data");
		// for(int i=0;i<jsonArray.size();i++) {
		// JSONObject jsonObject1 = jsonArray.getJSONObject(i);
		// jsonObject1.put("dt", dt);
		// jsonObject1.put("dt", dt);
		// jsonObject1.put("countryCode", countryCode);
		// out.collect(jsonObject1.toJSONString());
		//
		// }
		//
		// }
		// });

		SingleOutputStreamOperator<Tuple2<Long, String>> watermarkData = text
				.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
						// TODO Auto-generated method stub
						JSONObject jsonObject = JSONObject.parseObject(value);
						String date = jsonObject.getString("dt");
						Long timestape = jsonObject.getLong("CurrentTimeTemp");
						String dt = date.replace(" ", "/");
						String countryCode = jsonObject.getString("countryCode");
						// JSONArray jsonArray = jsonObject.getJSONArray("data");
						out.collect(new Tuple2<Long, String>(timestape, countryCode));
					}
				}).assignTimestampsAndWatermarks(new MyWatermark());

		SingleOutputStreamOperator<WordWithcount> alldata = watermarkData
				.map(new MapFunction<Tuple2<Long, String>, WordWithcount>() {

					@Override
					public WordWithcount map(Tuple2<Long, String> value) throws Exception {
						// TODO Auto-generated method stub
						Long count = value.f0;
						String word = value.f1;
						return new WordWithcount(word, 1);
					}
				}).keyBy("word").window(TumblingEventTimeWindows.of(Time.seconds(10))).sum("count");

		alldata.print();
		 //watermarkData.print();

		String outTopic = "demo1";

		Properties outprop = new Properties();

		outprop.setProperty("bootstrap.servers", "192.168.3.122:9092");
		prop.setProperty("transaction.timeout.ms", 60000 * 15 + "");

		@SuppressWarnings({ "deprecation", "unused" })
		// FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(outTopic,
		// new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()) ,
		// outprop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(outTopic,
				new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), outprop);

		DataStream<String>	watermarkinputdata=watermarkData.map(new MapFunction<Tuple2<Long,String>, String>() {

			@Override
			public String map(Tuple2<Long, String> value) throws Exception {
				// TODO Auto-generated method stub
				
				return value.f0+","+value.f1;
			}
		});
		
		//watermarkinputdata.addSink(myProducer);
		env.execute("FlinkKafkaTestETL");

	}

	public static class WordWithcount {
		public String word;
		public long count;

		public WordWithcount() {
		}

		public WordWithcount(String word, long count) {
			this.count = count;
			this.word = word;
		}

		@Override
		public String toString() {
			return "WordWithcount{" + "word='" + word + '\'' + ", count=" + count + '}';
		}
	}
}
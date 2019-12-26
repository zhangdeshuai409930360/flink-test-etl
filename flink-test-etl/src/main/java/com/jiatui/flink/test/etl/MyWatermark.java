package com.jiatui.flink.test.etl;

import java.text.SimpleDateFormat;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;



public class MyWatermark implements AssignerWithPeriodicWatermarks<Tuple2<Long,String>>{
	Long currentMaxTimestamp= 0L;
	final Long maxOutOfOrderness =10000L; //允许最大的乱序时间
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	@Override
	public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp) {
		// TODO Auto-generated method stub
		Long timestamp= element.f0;
		currentMaxTimestamp=Math.max(timestamp, currentMaxTimestamp);
		Long watermark=currentMaxTimestamp-maxOutOfOrderness;
		System.out.println("currentMaxTimestamp:"+sdf.format(currentMaxTimestamp)+"  "+"timestamp"+sdf.format(timestamp)+"  "+"watermark:"+sdf.format(watermark));
		
		return timestamp;
	}
	@Override
	public Watermark getCurrentWatermark() {
		// TODO Auto-generated method stub
		return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
		
	}

}

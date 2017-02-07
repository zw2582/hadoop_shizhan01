package cn.itcase.bigdata.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对统计流量总数排序
 * @author Administrator
 *
 */
public class FlowCountSort {

	static class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
		
		FlowBean flowBean = new FlowBean();
		Text v = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String line = value.toString();
			
			String[] split = line.split("\t");
			
			String phone = split[0];
			long upflow = Long.valueOf(split[1]);
			long downflow = Long.valueOf(split[2]);
			
			flowBean.setFlowBean(upflow, downflow);
			v.set(phone);
			
			context.write(flowBean, v);
		}
	}
	
	static class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
		
		protected void reduce(FlowBean flowbean, Iterable<Text> phones, Context context) throws java.io.IOException ,InterruptedException {
			context.write(phones.iterator().next(), flowbean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowCountSort.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outpath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outpath)) {
			fs.delete(outpath);
		}
		
		FileOutputFormat.setOutputPath(job, outpath);
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1); 
	}
	
}

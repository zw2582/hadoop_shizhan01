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
 * 统计手机上行流量总数，下行流量总数，以及流量总数
 * @author Administrator
 *
 */
public class FlowCount {

	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		FlowBean flow = new FlowBean();
		Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] split = line.split("\t");
			int length = split.length;
			long upflow = Long.valueOf(split[length-2]);
			long downflow = Long.valueOf(split[length-3]);
			
			flow.setFlowBean(upflow, downflow);
			v.set(split[0]);
			
			context.write(v, flow);
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		
		FlowBean flow = new FlowBean();
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> value, Context context)
				throws IOException, InterruptedException {
			
			for(FlowBean b : value) {
				flow.setFlowBean(flow.getUpflow() + b.getUpflow(), flow.getDownflow() + b.getDownflow());
			}
			
			context.write(key, flow);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
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

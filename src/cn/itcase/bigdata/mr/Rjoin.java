package cn.itcase.bigdata.mr;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Rjoin {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Rjoin.class);
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);
		
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
	
	/**
	 * mapper
	 * @author Administrator
	 *
	 */
	static class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		
		public InfoBean infoBean = new InfoBean();
		Text mapkey = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String line = value.toString();
			String[] data = line.split("\t");
			
			FileSplit is = (FileSplit) context.getInputSplit();
			String name = is.getPath().getName();
			
			if(name.startsWith("order")) {
				infoBean.setOrder(Integer.valueOf(data[0]), data[1], data[2], Integer.valueOf(data[3]));
			} else {
				infoBean.setProduct(data[0], data[1], Float.valueOf(data[2]), Integer.valueOf(data[3]));
			}
			mapkey.set(infoBean.getP_id());
			context.write(mapkey, infoBean);
		}
	}
	
	static class JoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
		
		protected void reduce(Text pid, Iterable<InfoBean> beans, Context context) throws java.io.IOException ,InterruptedException {
			InfoBean pBean = new InfoBean();
			ArrayList<InfoBean> listObean = new ArrayList<InfoBean>();
			for(InfoBean bean : beans) {
				if(bean.getFlag() == 2) {
					try {
						BeanUtils.copyProperties(pBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				} else {
					InfoBean obean = new InfoBean();
					try {
						BeanUtils.copyProperties(obean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					listObean.add(obean);
				}
			}
			
			for(InfoBean bean : listObean) {
				bean.setProduct(pBean.getP_id(), pBean.getP_name(), pBean.getP_price(), pBean.getP_count());
				context.write(bean, NullWritable.get());
			}
		};
	}

}

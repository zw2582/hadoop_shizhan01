package cn.itcase.bigdata.wc;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.hash.Hash;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final Log LOG =
		    LogFactory.getLog(WordCountMapper.class);
		  
		  private static boolean nativeCodeLoaded = false;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] st = value.toString().split(" ");
		for(String s : st) {
			context.write(new Text(s), new IntWritable(1));
		}
	}
	
	static {
	    // Try to load native hadoop library and set fallback flag appropriately
	    if(LOG.isDebugEnabled()) {
	      LOG.debug("Trying to load the custom-built native-hadoop library...");
	    }
	    try {
	      System.loadLibrary("hadoop");
	      LOG.debug("Loaded the native-hadoop library");
	      nativeCodeLoaded = true;
	    } catch (Throwable t) {
	      // Ignore failure to load
	      if(LOG.isDebugEnabled()) {
	        LOG.debug("Failed to load native-hadoop with error: " + t);
	        LOG.debug("java.library.path=" +
	            System.getProperty("java.library.path"));
	      }
	    }
	    
	    if (!nativeCodeLoaded) {
	      LOG.warn("Unable to load native-hadoop library for your platform... " +
	               "using builtin-java classes where applicable");
	    }
	  }
	
	public static void main(String[] args) {
		System.out.println(" vendor+max ".hashCode());
		System.out.println("*********");
		new NativeCodeLoader();
	}
}

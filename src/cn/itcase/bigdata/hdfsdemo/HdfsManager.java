package cn.itcase.bigdata.hdfsdemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


public class HdfsManager {

	private Configuration conf;
	private FileSystem fs;

	@Before
	public void init() throws IOException {
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.220.1:9000");
		fs = FileSystem.get(conf);
	}
	
	@Test
	public void testUpload() throws Exception {
		fs.copyFromLocalFile(new Path("D:/workspace/hadoop_shizhan01/src/cn/itcase/bigdata/hdfsdemo/nidaye.txt"), new Path("/"));
	}
	
	@Test
	public void testDownload() throws Exception {
		fs.copyToLocalFile(new Path("/nidaye.txt"), new Path("D:/Documents/"));
	}
	
	@Test
	public void testStreamUpload() throws Exception {
		FSDataOutputStream output = fs.create(new Path("/"), true);
		FileInputStream inputStream = new FileInputStream(new File("d:/Documents/dna_shop.sql"));
		
		IOUtils.copy(inputStream, output);
	}
	
	public void testStreamDownload() throws Exception{
		FSDataInputStream input = fs.open(new Path("/dna_shop.sql"));
		FileOutputStream output = new FileOutputStream(new File("d:/Documents/dna_shop2.sql"));
		
		IOUtils.copy(input, output);
	}
	
	public void testAccessDownload() throws Exception{
		FSDataInputStream input = fs.open(new Path("/dna_shop.sql"));
		input.seek(12);
		
		IOUtils.copy(input, System.out);
	}
}

package cn.itcase.bigdata.hdfsdemo;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.itcase.bigdata.service.IDatanode;

public class RpcClient {

	public static void main(String[] args) throws IOException {
		System.out.println(IDatanode.class.getClassLoader());
		
		IDatanode proxy = RPC.getProxy(IDatanode.class, 1L, new InetSocketAddress("localhost", 8888), 
				new Configuration());
		
		String name = proxy.getName();
		
		System.out.println(name);
	}
}

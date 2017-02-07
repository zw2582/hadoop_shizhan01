package cn.itcase.bigdata.serviceimp;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import cn.itcase.bigdata.service.IDatanode;

public class ServicePublier {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws  
	 */
	public static void main(String[] args) throws Exception {
		Builder builder = new RPC.Builder(new Configuration());
		builder.setBindAddress("localhost")
		.setProtocol(IDatanode.class)
		.setPort(8888).setInstance(new DatanodeImp());
		
		Server server = builder.build();
		server.start();
	}

}

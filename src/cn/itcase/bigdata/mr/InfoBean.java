package cn.itcase.bigdata.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InfoBean implements Writable{

	private int order_id;
	private String date = "";
	private String p_id = "";
	private int o_count;
	private String p_name ="";
	private float p_price;
	private int p_count;
	private int flag; //1.order,2.product
	
	public void setOrder(int order_id, String date, String p_id, int o_count) {
		this.order_id = order_id;
		this.date = date;
		this.p_id = p_id;
		this.o_count = o_count;
		this.flag = 1;
	}
	
	public void setProduct(String p_id, String p_name, float p_price, int p_count){
		this.p_id = p_id;
		this.p_name= p_name;
		this.p_price = p_price;
		this.p_count = p_count;
		this.flag = 2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id);
		out.writeUTF(date);
		out.writeUTF(p_id);
		out.writeInt(o_count);
		out.writeUTF(p_name);
		out.writeFloat(p_price);
		out.writeInt(p_count);
		out.writeInt(flag);
	}

	public void readFields(DataInput in) throws IOException {
		order_id=in.readInt();
		date=in.readUTF();
		p_id=in.readUTF();
		o_count=in.readInt();
		p_name=in.readUTF();
		p_price=in.readFloat();
		p_count=in.readInt();
		flag=in.readInt();
	}
	
	@Override
	public String toString() {
		return "order_id:"+order_id+"\t"
	+"date:"+date+"\t"
	+"p_id:"+p_id+"\t"
	+"o_count:"+o_count+"\t"
	+"p_name:"+p_name+"\t"
	+"p_price:"+p_price+"\t"
	+"p_count:"+p_count;
	}

	public int getOrder_id() {
		return order_id;
	}

	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getP_id() {
		return p_id;
	}

	public void setP_id(String p_id) {
		this.p_id = p_id;
	}

	public int getO_count() {
		return o_count;
	}

	public void setO_count(int o_count) {
		this.o_count = o_count;
	}

	public String getP_name() {
		return p_name;
	}

	public void setP_name(String p_name) {
		this.p_name = p_name;
	}

	public float getP_price() {
		return p_price;
	}

	public void setP_price(float p_price) {
		this.p_price = p_price;
	}

	public int getP_count() {
		return p_count;
	}

	public void setP_count(int p_count) {
		this.p_count = p_count;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}
	
	
}

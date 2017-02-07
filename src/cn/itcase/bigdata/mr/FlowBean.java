package cn.itcase.bigdata.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 流量对象
 * @author Administrator
 *
 */
public class FlowBean implements WritableComparable<FlowBean>{

	private long upflow;
	
	private long downflow;
	
	private long totalflow;
	
	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	public long getDownflow() {
		return downflow;
	}

	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}

	public long getTotalflow() {
		return totalflow;
	}

	public FlowBean(){}
	
	public FlowBean(long upflow, long downflow){
		this.upflow=upflow;
		this.downflow=downflow;
		this.totalflow = this.upflow + this.downflow;
	}
	
	public void setFlowBean(long upflow, long downflow){
		this.upflow=upflow;
		this.downflow=downflow;
		this.totalflow = this.upflow + this.downflow;
	}

	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.totalflow = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(this.upflow);
		out.writeLong(downflow);
		out.writeLong(this.totalflow);
	}

	public int compareTo(FlowBean o) {
		return this.totalflow > o.getTotalflow() ? -1 : 1;
	}
}

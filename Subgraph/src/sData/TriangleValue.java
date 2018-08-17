package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TriangleValue implements Writable {

    public OneNodeKey node3;
    public TwoNodeKey node12;

    public TriangleValue() {
	this.node3 = new OneNodeKey();
	this.node12 = new TwoNodeKey();
    }

    public TriangleValue(OneNodeKey node3, TwoNodeKey node12) {
	this.node3 = node3;
	this.node12 = node12;
    }

    public void set(OneNodeKey node3, TwoNodeKey node12) {
	this.node3 = node3;
	this.node12 = node12;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node12.readFields(in);
	node3.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	node12.write(out);
	node3.write(out);
    }

}

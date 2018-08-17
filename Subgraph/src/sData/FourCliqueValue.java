package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FourCliqueValue implements Writable {

    public TwoNodeKey node12;
    public TwoNodeKey node34;

    public FourCliqueValue() {
	this.node34 = new TwoNodeKey();
	this.node12 = new TwoNodeKey();
    }

    public FourCliqueValue(TwoNodeKey node12, TwoNodeKey node34) {
	this.node34 = node34;
	this.node12 = node12;
    }

    public void set(TwoNodeKey node12, TwoNodeKey node34) {
	this.node34 = node34;
	this.node12 = node12;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node12.readFields(in);
	node34.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	node12.write(out);
	node34.write(out);
    }
}

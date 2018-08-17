package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TwoNodeKeyIntDegreeArray implements Writable {

    public TwoNodeKey key;
    public IntDegreeArray array;

    public TwoNodeKeyIntDegreeArray() {
	key = new TwoNodeKey();
	array = new IntDegreeArray();
    }

    public TwoNodeKeyIntDegreeArray(TwoNodeKey key, IntDegreeArray array) {
	this.key = key;
	this.array = array;
    }

    public void set(TwoNodeKey key, IntDegreeArray array) {
	this.key = key;
	this.array = array;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	key.readFields(in);
	array.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	key.write(out);
	array.write(out);
    }
}

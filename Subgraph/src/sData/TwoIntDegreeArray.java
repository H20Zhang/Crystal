package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TwoIntDegreeArray implements Writable {

    public IntDegreeArray array1;
    public IntDegreeArray array2;

    public TwoIntDegreeArray() {
	array1 = null;
	array2 = null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	array1 = new IntDegreeArray();
	array1.readFields(in);

	array2 = new IntDegreeArray();
	array2.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
	array1.write(out);
	array2.write(out);

    }

}

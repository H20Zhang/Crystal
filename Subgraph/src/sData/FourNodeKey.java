package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.Writable;

public class FourNodeKey implements Serializable, Writable {

    /**
     * 
     */
    private static final long serialVersionUID = -4409533337058314687L;

    public int node1;
    public int node2;
    public int node3;
    public int node4;

    public int degree1;
    public int degree2;
    public int degree3;
    public int degree4;

    public FourNodeKey() {
	node1 = 0;
	node2 = 0;
	node3 = 0;
	node4 = 0;

	degree1 = 0;
	degree2 = 0;
	degree3 = 0;
	degree4 = 0;
    }

    public void set(OneNodeKey key1, OneNodeKey key2, OneNodeKey key3, OneNodeKey key4) {
	node1 = key1.node;
	node2 = key2.node;
	node3 = key3.node;
	node4 = key4.node;

	degree1 = key1.degree;
	degree2 = key2.degree;
	degree3 = key3.degree;
	degree4 = key4.degree;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node1 = in.readInt();
	node2 = in.readInt();
	node3 = in.readInt();
	node4 = in.readInt();

	degree1 = in.readInt();
	degree2 = in.readInt();
	degree3 = in.readInt();
	degree4 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(node1);
	out.writeInt(node2);
	out.writeInt(node3);
	out.writeInt(node4);

	out.writeInt(degree1);
	out.writeInt(degree2);
	out.writeInt(degree3);
	out.writeInt(degree4);
    }
}

package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class TwoIntArray implements Serializable, Writable {

    private static final long serialVersionUID = -981573633227065018L;
    public int size1;
    public int size2;
    public int nodeArray1[];
    public int nodeArray2[];

    public TwoIntArray() {
	this.size1 = 0;
	this.nodeArray1 = null;
	this.size2 = 0;
	this.nodeArray2 = null;
    }

    public TwoIntArray(int size1, int size2) {
	this.size1 = size1;
	this.nodeArray1 = new int[size1];
	this.size2 = size2;
	this.nodeArray2 = new int[size2];
    }

    public int getNode1(int i) {
	return nodeArray1[i];
    }

    public int getNode2(int i) {
	return nodeArray2[2];
    }

    public void setNode1(int pos, int node) {
	nodeArray1[pos] = node;
    }

    public void setNode2(int pos, int node) {
	nodeArray2[pos] = node;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	size1 = in.readInt();
	size2 = in.readInt();
	nodeArray1 = new int[size1];
	for (int i = 0; i < size1; ++i) {
	    nodeArray1[i] = in.readInt();
	}

	nodeArray2 = new int[size2];
	for (int i = 0; i < size2; ++i) {
	    nodeArray2[i] = in.readInt();
	}

    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(size1);
	out.writeInt(size2);
	for (int i = 0; i < size1; ++i) {
	    out.writeInt(nodeArray1[i]);
	}

	for (int i = 0; i < size2; ++i) {
	    out.writeInt(nodeArray2[i]);
	}
    }

}
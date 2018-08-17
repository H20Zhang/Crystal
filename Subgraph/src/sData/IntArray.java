package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class IntArray implements Serializable, Writable {

    private static final long serialVersionUID = -6807229069072434104L;

    public int size;
    public int nodeArray[];

    public IntArray() {
	this.size = 0;
	this.nodeArray = null;
    }

    public IntArray(int size) {
	this.size = size;
	this.nodeArray = new int[size];
    }

    public int getNode(int i) {
	return nodeArray[i];
    }

    public void setNode(int pos, int node) {
	nodeArray[pos] = node;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	size = in.readInt();
	nodeArray = new int[size];
	for (int i = 0; i < size; ++i) {
	    nodeArray[i] = in.readInt();
	}
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(size);
	for (int i = 0; i < size; ++i) {
	    out.writeInt(nodeArray[i]);
	}
    }

    @Override
    public String toString() {
	return "IntDegreeArray [size=" + size + ", nodeArray=" + Arrays.toString(nodeArray) + "]";
    }

}

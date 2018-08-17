package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class IntDegreeArray implements Serializable, Writable {

    public int size;
    public int nodeArray[];
    public int degreeArray[];

    private static final long serialVersionUID = -2533420664006285670L;

    public IntDegreeArray() {
	this.size = 0;
	this.nodeArray = null;
	this.degreeArray = null;
    }

    public IntDegreeArray(int size) {
	this.size = size;
	this.nodeArray = new int[size];
	this.degreeArray = new int[size];
    }

    public int getNode(int i) {
	return nodeArray[i];
    }

    public int getDegree(int i) {
	return degreeArray[i];
    }

    public void addNodeAndDegree(int pos, int node, int degree) {
	nodeArray[pos] = node;
	degreeArray[pos] = degree;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	size = in.readInt();
	nodeArray = new int[size];
	degreeArray = new int[size];
	for (int i = 0; i < size; ++i) {
	    nodeArray[i] = in.readInt();
	    degreeArray[i] = in.readInt();
	}
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(size);
	for (int i = 0; i < size; ++i) {
	    out.writeInt(nodeArray[i]);
	    out.writeInt(degreeArray[i]);
	}
    }

    @Override
    public String toString() {
	return "IntDegreeArray [size=" + size + ", nodeArray=" + Arrays.toString(nodeArray) + ", degreeArray="
		+ Arrays.toString(degreeArray) + "]";
    }

}

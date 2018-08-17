package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoNodeKey implements WritableComparable<TwoNodeKey> {

    public int node1;
    public int node2;
    public int node1Degree;
    public int node2Degree;

    public int getNode1() {
	return node1;
    }

    public void setNode1(int node1) {
	this.node1 = node1;
    }

    public int getNode2() {
	return node2;
    }

    public void setNode2(int node2) {
	this.node2 = node2;
    }

    public int getNode1Degree() {
	return node1Degree;
    }

    public void setNode1Degree(int node1Degree) {
	this.node1Degree = node1Degree;
    }

    public int getNode2Degree() {
	return node2Degree;
    }

    public void setNode2Degree(int node2Degree) {
	this.node2Degree = node2Degree;
    }

    public void set(OneNodeKey first, OneNodeKey second) {
	node1 = first.node;
	node1Degree = first.degree;
	node2 = second.node;
	node2Degree = second.degree;
    }

    public TwoNodeKey() {
	node1 = 0;
	node2 = 0;
	node1Degree = 0;
	node2Degree = 0;
    }

    public TwoNodeKey(OneNodeKey first, OneNodeKey second) {
	node1 = first.node;
	node1Degree = first.degree;
	node2 = second.node;
	node2Degree = second.degree;
    }

    public TwoNodeKey(int aNode1, int aNode2, int aNode1Degree, int aNode2Degree) {
	node1 = aNode1;
	node2 = aNode2;
	node1Degree = aNode1Degree;
	node2Degree = aNode2Degree;
    }

    public TwoNodeKey(TwoNodeKey other) {
	node1 = other.node1;
	node2 = other.node2;
	node1Degree = other.node1Degree;
	node2Degree = other.node2Degree;
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(node1);
	out.writeInt(node2);
	out.writeInt(node1Degree);
	out.writeInt(node2Degree);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node1 = in.readInt();
	node2 = in.readInt();
	node1Degree = in.readInt();
	node2Degree = in.readInt();
    }

    @Override
    public String toString() {
	return "(" + node1 + "," + node2 + "Degree:" + node1Degree + "," + node2Degree + ")";
    }

    @Override
    public int compareTo(TwoNodeKey other) {
	if (node1 > other.node1) {
	    return 1;
	}
	if (node1 < other.node1) {
	    return -1;
	}

	if (node2 > other.node2) {
	    return 1;
	}

	if (node2 < other.node2) {
	    return -1;
	}
	return 0;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + node1;
	result = prime * result + node2;
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	TwoNodeKey other = (TwoNodeKey) obj;
	if (node1 != other.node1)
	    return false;
	if (node2 != other.node2)
	    return false;
	return true;
    }

    public static class TwoNodeKeyComparator extends WritableComparator {
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    int i1 = readInt(b1, s1);
	    int i2 = readInt(b2, s2);

	    int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
	    if (0 != comp)
		return comp;

	    int j1 = readInt(b1, s1 + 4);
	    int j2 = readInt(b2, s2 + 4);
	    comp = (j1 < j2) ? -1 : (j1 == j2) ? 0 : 1;

	    return comp;
	}
    }

}

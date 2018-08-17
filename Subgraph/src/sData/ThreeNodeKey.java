package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ThreeNodeKey implements Serializable, WritableComparable<ThreeNodeKey> {

    /**
     * 
     */
    private static final long serialVersionUID = 8965202801036834053L;

    public int node1;
    public int node2;
    public int node3;

    public int degree1;
    public int degree2;
    public int degree3;

    public ThreeNodeKey() {
	this.node1 = -1;
	this.node2 = -1;
	this.node3 = -1;
	this.degree1 = -1;
	this.degree2 = -1;
	this.degree3 = -1;
    }

    public ThreeNodeKey(int node1, int node2, int node3, int degree1, int degree2, int degree3) {
	super();
	this.node1 = node1;
	this.node2 = node2;
	this.node3 = node3;
	this.degree1 = degree1;
	this.degree2 = degree2;
	this.degree3 = degree3;
    }

    public void set(int node1, int node2, int node3, int degree1, int degree2, int degree3) {
	this.node1 = node1;
	this.node2 = node2;
	this.node3 = node3;
	this.degree1 = degree1;
	this.degree2 = degree2;
	this.degree3 = degree3;
    }

    public void set(OneNodeKey first, OneNodeKey second, OneNodeKey third) {
	this.node1 = first.node;
	this.node2 = second.node;
	this.node3 = third.node;
	this.degree1 = first.degree;
	this.degree2 = second.degree;
	this.degree3 = third.degree;
    }

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

    public int getNode3() {
	return node3;
    }

    public void setNode3(int node3) {
	this.node3 = node3;
    }

    public int getDegree1() {
	return degree1;
    }

    public void setDegree1(int degree1) {
	this.degree1 = degree1;
    }

    public int getDegree2() {
	return degree2;
    }

    public void setDegree2(int degree2) {
	this.degree2 = degree2;
    }

    public int getDegree3() {
	return degree3;
    }

    public void setDegree3(int degree3) {
	this.degree3 = degree3;
    }

    @Override
    public String toString() {
	return "ThreeNodeKey [node1=" + node1 + ", node2=" + node2 + ", node3=" + node3 + ", degree1=" + degree1
		+ ", degree2=" + degree2 + ", degree3=" + degree3 + "]";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node1 = in.readInt();
	node2 = in.readInt();
	node3 = in.readInt();
	degree1 = in.readInt();
	degree2 = in.readInt();
	degree3 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(node1);
	out.writeInt(node2);
	out.writeInt(node3);
	out.writeInt(degree1);
	out.writeInt(degree2);
	out.writeInt(degree3);
    }

    @Override
    public int compareTo(ThreeNodeKey other) {

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

	if (node3 > other.node3) {
	    return 1;
	}

	if (node3 < other.node3) {
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
	result = prime * result + node3;
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
	ThreeNodeKey other = (ThreeNodeKey) obj;
	if (node1 != other.node1)
	    return false;
	if (node2 != other.node2)
	    return false;
	if (node3 != other.node3)
	    return false;
	return true;
    }

    public static class ThreeNodeKeyComparator extends WritableComparator {
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
	    if (0 != comp)
		return comp;

	    int k1 = readInt(b1, s1 + 8);
	    int k2 = readInt(b2, s2 + 8);
	    comp = (k1 < k2) ? -1 : (k1 == k2) ? 0 : 1;

	    return comp;
	}
    }

    public static void main(String[] args) {

    }

}

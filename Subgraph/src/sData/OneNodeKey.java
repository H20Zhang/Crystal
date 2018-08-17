package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OneNodeKey implements Serializable, WritableComparable<OneNodeKey> {

    /**
     * 
     */
    private static final long serialVersionUID = 6924719206698831558L;

    public int node;
    public int degree;

    public OneNodeKey() {

    }

    public OneNodeKey(OneNodeKey other) {
	node = other.node;
	degree = other.degree;
    }

    public void set(int node1, int degree1) {
	this.node = node1;
	this.degree = degree1;
    }

    public int getNode() {
	return node;
    }

    public void setNode(int node1) {
	this.node = node1;
    }

    public int getDegree() {
	return degree;
    }

    public void setDegree(int degree1) {
	this.degree = degree1;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	node = in.readInt();
	degree = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(node);
	out.writeInt(degree);
    }

    @Override
    public int compareTo(OneNodeKey other) {
	if (node > other.node) {
	    return 1;
	}
	if (node < other.node) {
	    return -1;
	}
	return 0;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + node;
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
	OneNodeKey other = (OneNodeKey) obj;
	if (node != other.node)
	    return false;
	return true;
    }

    public static class OneNodeKeyComparator extends WritableComparator {
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    int i1 = readInt(b1, s1);
	    int i2 = readInt(b2, s2);

	    int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
	    return comp;

	}
    }

    public static class OneNodeKeyJavaComparator implements Comparator<OneNodeKey> {

	@Override
	public int compare(OneNodeKey o1, OneNodeKey o2) {
	    if (o1.node > o2.node) {
		return 1;
	    }
	    if (o1.node < o2.node) {
		return -1;
	    }
	    return 0;
	}

    }

    public static void main(String[] args) {
	OneNodeKey a = new OneNodeKey();
	a.set(1, 2);

	OneNodeKey b = new OneNodeKey();
	b.set(4, 3);

	OneNodeKey c = new OneNodeKey();
	c.set(3, 2);

	List<OneNodeKey> aList = new ArrayList<OneNodeKey>(3);
	aList.add(a);
	aList.add(b);
	aList.add(c);

	aList.sort(new OneNodeKeyJavaComparator());

	System.out.println(aList);

    }

}

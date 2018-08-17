package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class EdgeWritable implements Serializable, WritableComparable<EdgeWritable> {

    private static final long serialVersionUID = -8420593836230815765L;
    private int node1;
    private int node2;

    public byte[] getBytes() {

	byte byteArray[] = new byte[8];
	/*
	 * int temp1 = (node1 * node2) / 2; int temp2 = node2 - node1;
	 * 
	 * byteArray[7] = (byte) ((temp1 >> 24) & 0xFF); byteArray[1] = (byte)
	 * ((temp1 >> 16) & 0xFF); byteArray[6] = (byte) ((temp1 >> 8) & 0xFF);
	 * byteArray[4] = (byte) ((temp1) & 0xFF); byteArray[3] = (byte) ((temp2
	 * >> 24) & 0xFF); byteArray[2] = (byte) ((temp2 >> 16) & 0xFF);
	 * byteArray[5] = (byte) ((temp2 >> 8) & 0xFF); byteArray[0] = (byte)
	 * ((temp2) & 0xFF);
	 * 
	 * byteArray[1] = (byte) (byteArray[1] & byteArray[3]); byteArray[2] =
	 * (byte) (byteArray[2] & byteArray[7]);
	 */

	byteArray[7] = (byte) ((node1 >> 24) & 0xFF);
	byteArray[1] = (byte) ((node1 >> 16) & 0xFF);
	byteArray[6] = (byte) ((node1 >> 8) & 0xFF);
	byteArray[4] = (byte) ((node1) & 0xFF);
	byteArray[3] = (byte) ((node2 >> 24) & 0xFF);
	byteArray[2] = (byte) ((node2 >> 16) & 0xFF);
	byteArray[5] = (byte) ((node2 >> 8) & 0xFF);
	byteArray[0] = (byte) ((node2) & 0xFF);
	return (byteArray);
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

    /**
     * Empty constructor - required for serialization.
     */
    public EdgeWritable() {

    }

    /**
     * Constructor with two String objects provided as input.
     */
    public EdgeWritable(int aNode1, int aNode2) {
	node1 = aNode1;
	node2 = aNode2;
    }

    /**
     * Serializes the fields of this object to out.
     */
    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(node1);
	out.writeInt(node2);
    }

    /**
     * Deserializes the fields of this object from in.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
	node1 = in.readInt();
	node2 = in.readInt();
    }

    /**
     * Compares this object to another StringPairWritable object by comparing
     * the left strings first. If the left strings are equal, then the right
     * strings are compared.
     */
    @Override
    public int compareTo(EdgeWritable other) {
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

    /**
     * A custom method that returns the two strings in the StringPairWritable
     * object inside parentheses and separated by a comma. For example:
     * "(left,right)".
     */
    @Override
    public String toString() {
	return "(" + node1 + "," + node2 + ")";
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	EdgeWritable other = (EdgeWritable) obj;
	if (node1 != other.node1)
	    return false;
	if (node2 != other.node2)
	    return false;
	return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + node1;
	result = prime * result + node2;
	return result;
    }

}

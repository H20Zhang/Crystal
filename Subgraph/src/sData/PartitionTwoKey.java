package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PartitionTwoKey implements WritableComparable<PartitionTwoKey> {

    public int key1;
    public int key2;

    public PartitionTwoKey() {
	key1 = 0;
	key2 = 0;
    }

    public PartitionTwoKey(int key1, int key2) {
	this.key1 = key1;
	this.key2 = key2;
    }

    public void setKey(TwoNodeKey key1, TwoNodeKey key2, Integer p) {
	this.key1 = Math.abs(key1.hashCode()) % p;
	this.key2 = Math.abs(key2.hashCode()) % p;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	key1 = in.readInt();
	key2 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(key1);
	out.writeInt(key2);
    }

    @Override
    public int compareTo(PartitionTwoKey other) {

	if (key1 > other.key1) {
	    return 1;
	}
	if (key1 < other.key1) {
	    return -1;
	}

	if (key2 > other.key2) {
	    return 1;
	}

	if (key2 < other.key2) {
	    return -1;
	}

	return 0;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + key1;
	result = prime * result + key2;
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
	PartitionTwoKey other = (PartitionTwoKey) obj;
	if (key1 != other.key1)
	    return false;
	if (key2 != other.key2)
	    return false;
	return true;
    }

}

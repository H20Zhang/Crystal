package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PartitionThreeKey implements WritableComparable<PartitionThreeKey> {

    public int key1;
    public int key2;
    public int key3;

    public PartitionThreeKey() {
	key1 = 0;
	key2 = 0;
	key3 = 0;
    }

    public void setKey(TwoNodeKey key1, TwoNodeKey key2, TwoNodeKey key3, Integer p) {
	this.key1 = Math.abs(key1.hashCode()) % p;
	this.key2 = Math.abs(key2.hashCode()) % p;
	this.key3 = Math.abs(key3.hashCode()) % p;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	key1 = in.readInt();
	key2 = in.readInt();
	key3 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(key1);
	out.writeInt(key2);
	out.writeInt(key3);
    }

    @Override
    public int compareTo(PartitionThreeKey other) {
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

	if (key3 > other.key3) {
	    return 1;
	}

	if (key3 < other.key3) {
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
	result = prime * result + key3;
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
	PartitionThreeKey other = (PartitionThreeKey) obj;
	if (key1 != other.key1)
	    return false;
	if (key2 != other.key2)
	    return false;
	if (key3 != other.key3)
	    return false;
	return true;
    }
}

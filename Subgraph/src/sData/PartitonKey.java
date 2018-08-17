package sData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PartitonKey implements WritableComparable<PartitonKey> {

    public int key;
    public int randomKey;

    public PartitonKey() {
	key = 0;
	randomKey = 0;
    }

    public PartitonKey(int key, int randomKey) {
	this.key = key;
	this.randomKey = randomKey;
    }

    public void setKey(TwoNodeKey key, int randomKey, Integer p, Integer m) {
	this.key = Math.abs(key.hashCode()) % p;
	this.randomKey = Math.abs(randomKey) % m;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	key = in.readInt();
	randomKey = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(key);
	out.writeInt(randomKey);
    }

    @Override
    public int compareTo(PartitonKey other) {
	if (key > other.key) {
	    return 1;
	}
	if (key < other.key) {
	    return -1;
	}

	if (randomKey > other.randomKey) {
	    return 1;
	}
	if (randomKey < other.randomKey) {
	    return -1;
	}

	return 0;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + key;
	result = prime * result + randomKey;
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
	PartitonKey other = (PartitonKey) obj;
	if (key != other.key)
	    return false;
	if (randomKey != other.randomKey)
	    return false;
	return true;
    }

}

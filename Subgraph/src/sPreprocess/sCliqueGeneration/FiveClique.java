package sPreprocess.sCliqueGeneration;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.ThreeNodeKey;
import sData.TwoNodeKey;

//unfinished, the following code is just the same as the FourClique algorithms



/**
 * Pattern name: Five Clique <br>
 * Pattern: (0,1),(0,2),(0,3),(0,4),(1,2),(1,3),(1,4),(2,3),(2,4),(3,4)
 * @author zhanghao
 * @version Unfinished
 */
public class FiveClique {
    public static class FiveCliqueAMapper extends Mapper<ThreeNodeKey, IntDegreeArray, ThreeNodeKey, OneNodeKey> {
	private final OneNodeKey aValue = new OneNodeKey();
	private final OneNodeKey key1 = new OneNodeKey();
	private final OneNodeKey key2 = new OneNodeKey();
	private final OneNodeKey key3 = new OneNodeKey();
	private final OneNodeKey key4 = new OneNodeKey();

	boolean partialGreater(OneNodeKey lhs, OneNodeKey rhs) {
	    if (lhs.degree > rhs.degree) {
		return true;
	    }

	    if (lhs.degree < rhs.degree) {
		return false;
	    }

	    if (lhs.node > rhs.node) {
		return true;
	    }

	    if (lhs.node < rhs.node) {
		return false;
	    }

	    return false;
	}

	@Override
	protected void map(ThreeNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    key1.set(key.getNode1(), key.getDegree1());
	    key2.set(key.getNode2(), key.getDegree2());
	    key3.set(key.getNode3(), key.getDegree3());

	    int size = value.size;

	    for (int i = 0; i < size; ++i) {
		key4.set(value.getNode(i), value.getDegree(i));

		if (partialGreater(key4, key1) && partialGreater(key4, key2) && partialGreater(key4, key3)) {
		    aValue.set(value.getNode(i), value.getDegree(i));
		    context.write(key, aValue);
		}
	    }
	}
    }

    public static class FiveCliqueFAReducer extends Reducer<ThreeNodeKey, OneNodeKey, TwoNodeKey, ThreeNodeKey> {

	private BloomFilter[] aBloomFilter;
	private final Key aKey = new Key();
	private final ArrayList<OneNodeKey> aVector = new ArrayList<OneNodeKey>(8192);
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private byte byteArray[] = new byte[8];

	private final TwoNodeKey outputKey = new TwoNodeKey();
	private final TwoNodeKey outputValue = new TwoNodeKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Integer NIBFp = 1;
	    Integer NIBFoffset = 0;

	    p = NIBFp;
	    offset = NIBFoffset;

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    aBloomFilter = new BloomFilter[allPaths.length];
	    for (int i = 0; i < allPaths.length; ++i) {
		aBloomFilter[i] = new BloomFilter();
	    }

	    for (int i = 0; i < allPaths.length; ++i) {

		if (i % NIBFp == NIBFoffset) {
		    FileInputStream in = new FileInputStream(new File(allPaths[i].toString()));
		    DataInputStream input = new DataInputStream(in);
		    aBloomFilter[i].readFields(input);
		}
	    }
	}

	public void getBytes(TwoNodeKey edge) {
	    int node1 = edge.getNode1();
	    int node2 = edge.getNode2();

	    byteArray[7] = (byte) ((node1 >> 24) & 0xFF);
	    byteArray[1] = (byte) ((node1 >> 16) & 0xFF);
	    byteArray[6] = (byte) ((node1 >> 8) & 0xFF);
	    byteArray[4] = (byte) ((node1) & 0xFF);
	    byteArray[3] = (byte) ((node2 >> 24) & 0xFF);
	    byteArray[2] = (byte) ((node2 >> 16) & 0xFF);
	    byteArray[5] = (byte) ((node2 >> 8) & 0xFF);
	    byteArray[0] = (byte) ((node2) & 0xFF);
	}

	@Override
	protected void reduce(ThreeNodeKey key, Iterable<OneNodeKey> values, Context context)
		throws IOException, InterruptedException {

	    for (OneNodeKey aInt : values) {
		aVector.add(new OneNodeKey(aInt));
	    }

	    for (int i = 0; i < aVector.size(); i++) {
		OneNodeKey a = aVector.get(i);
		for (int j = i + 1; j < aVector.size(); j++) {
		    OneNodeKey b = aVector.get(j);
		    int leading = a.getNode();
		    if (a.getNode() < b.getNode()) {
			outputKey.set(a, b);
		    } else {
			outputKey.set(b, a);
			leading = b.getNode();
		    }

		    getBytes(outputKey);
		    aKey.set(byteArray, 1);

		    if ((leading % 240) % p == offset) {
			context.getCounter("count", "total").increment(1);
			if (aBloomFilter[leading % 240].membershipTest(aKey)) {
			    context.getCounter("count", "tri").increment(1);
			    context.write(outputKey, key);
			    continue;
			}
		    }
		}
	    }
	    aVector.clear();
	}
    }

    public static class FiveCliqueFBMapper extends Mapper<TwoNodeKey, ThreeNodeKey, TwoNodeKey, ThreeNodeKey> {
	@Override
	protected void map(TwoNodeKey key, ThreeNodeKey value, Context context)
		throws IOException, InterruptedException {
	    context.write(key, value);
	}
    }

    public static class FiveCliqueFBReducer extends Reducer<TwoNodeKey, ThreeNodeKey, TwoNodeKey, ThreeNodeKey> {
	private final List<ThreeNodeKey> aVector = new ArrayList<ThreeNodeKey>(20480);

	private Boolean isOutput = true;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    isOutput = context.getConfiguration().getBoolean("test.isOutput", true);
	}

	@Override
	protected void reduce(TwoNodeKey key, Iterable<ThreeNodeKey> values, Context context)
		throws IOException, InterruptedException {
	    boolean containZero = false;
	    for (ThreeNodeKey aInt : values) {
		if (aInt.getNode1() == -1) {
		    containZero = true;
		    continue;
		}
		aVector.add(WritableUtils.clone(aInt, context.getConfiguration()));
	    }

	    if (containZero == true && isOutput == true) {
		context.getCounter("count", "tri").increment(aVector.size());
		for (ThreeNodeKey integer : aVector) {
		    context.write(key, integer);
		}
	    }
	    aVector.clear();
	}
    }
}

/*
 * public static class FiveCliqueFCMapper extends Mapper<TwoNodeKey,
 * ThreeNodeKey, ThreeNodeKey, OneNodeKey>{
 * 
 * private final ThreeNodeKey outputKey = new ThreeNodeKey(); private final
 * OneNodeKey first = new OneNodeKey(); private final OneNodeKey second = new
 * OneNodeKey(); private final OneNodeKey third = new OneNodeKey(); private
 * final OneNodeKey fourth = new OneNodeKey(); private final List<OneNodeKey>
 * sortArray = new ArrayList<OneNodeKey>(4);
 * 
 * @Override protected void map(TwoNodeKey key, TwoNodeKey value,Context
 * context) throws IOException, InterruptedException { first.set(key.getNode1(),
 * key.getNode1Degree()); second.set(key.getNode2(), key.getNode2Degree());
 * third.set(value.getNode1(), value.getNode1Degree());
 * fourth.set(value.getNode2(), value.getNode2Degree());
 * 
 * // 2 3 4 | 1 sortArray.add(second); sortArray.add(third);
 * sortArray.add(fourth); sortArray.sort(null); outputKey.set(sortArray.get(0),
 * sortArray.get(1), sortArray.get(2)); context.write(outputKey, first);
 * 
 * // 1 3 4 | 2 sortArray.add(first); sortArray.add(third);
 * sortArray.add(fourth); sortArray.sort(null); outputKey.set(sortArray.get(0),
 * sortArray.get(1), sortArray.get(2)); context.write(outputKey, second);
 * 
 * //1 2 4 | 3 sortArray.add(first); sortArray.add(second);
 * sortArray.add(fourth); sortArray.sort(null); outputKey.set(sortArray.get(0),
 * sortArray.get(1), sortArray.get(2)); context.write(outputKey, third);
 * 
 * // 1 2 3 | 4 sortArray.add(first); sortArray.add(second);
 * sortArray.add(third); sortArray.sort(null); outputKey.set(sortArray.get(0),
 * sortArray.get(1), sortArray.get(2)); context.write(outputKey, fourth); } }
 * 
 * public static class FiveCliqueFCReducer extends Reducer<ThreeNodeKey,
 * OneNodeKey, ThreeNodeKey, IntDegreeArray>{ private final List<OneNodeKey>
 * list = new ArrayList<OneNodeKey>();
 * 
 * @Override protected void reduce(ThreeNodeKey key, Iterable<OneNodeKey>
 * values, Context context) throws IOException, InterruptedException { for
 * (OneNodeKey oneNodeKey : values) { list.add(new OneNodeKey(oneNodeKey)); }
 * 
 * IntDegreeArray outputValue = new IntDegreeArray(list.size()); int pos = 0;
 * for (OneNodeKey oneNodeKey : list) { outputValue.addNodeAndDegree(pos++,
 * oneNodeKey.getNode(), oneNodeKey.getDegree()); }
 * 
 * context.write(key, outputValue); list.clear(); } } }
 * 
 */

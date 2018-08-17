package sAlg;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;



/**
 * <h1>
 * Output House
 * </h1>
 * <ul>
 * 	<li>Pattern name: House </li>
 * 	<li>Pattern: (0,1),(0,3),(0,4),(1,2),(2,3),(3,4)  </li>
 * 	<li>Core: (0,1,3)</li>
 * 	<li>Non Core Node: [2,4]</li>
 * </ul>
 * @author zhanghao
 */

public class HouseSO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(HouseSO.class);

    public static String appendNumberToLengthThree(Integer i) {
	if (i > 0) {
	    if (i < 10) {
		return "00" + i;
	    } else if (i < 100) {
		return "0" + i;
	    } else {
		return i.toString();
	    }
	} else {
	    return "000";
	}
    }

    public static class HouseOMapper extends Mapper<TwoNodeKey, IntDegreeArray, IntArray, TwoIntArray> {
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private boolean isOutput = false;

	private final TIntObjectHashMap<TIntArrayList> map = new TIntObjectHashMap<TIntArrayList>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
		Integer NIBFp = conf.getInt("test.p", 1);
		Integer NIBFoffset = conf.getInt("test.offset", 0);
	    isOutput = conf.getBoolean("test.isOutput", false);

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    p = NIBFp;
	    offset = NIBFoffset;

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);

	    ArrayList<Integer> shuffleList = new ArrayList<Integer>();
	    for (int i = 0; i < 240; ++i) {

		if ((i % p) == offset) {
		    shuffleList.add(i);
		}
	    }

	    String path = conf.get("test.line") + "/part-r-00";
	    // Collections.shuffle(shuffleList, new
	    // Random(System.currentTimeMillis()));

	    for (int k = 0; k < shuffleList.size(); k++) {

		reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[k], conf);
		OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

		long position = reader.getPosition();
		while (reader.next(key, value)) {
		    String syncSeen = reader.syncSeen() ? "*" : "";
		    // System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		    // key, value);
		    position = reader.getPosition(); // beginning of next record
		    int theCopiedKey = key.getNode();

		    for (int h = 0; h < value.size; ++h) {
			int keyToAddMap = value.getNode(h);
			if (map.containsKey(keyToAddMap)) {
			    map.get(keyToAddMap).add(theCopiedKey);

			} else {
			    TIntArrayList vi = new TIntArrayList();
			    vi.add(theCopiedKey);
			    map.put(keyToAddMap, vi);
			}
		    }
		}
		IOUtils.closeStream(reader);
	    }

	    // TIntIterator iterator = map.keySet().iterator();
	    // while (iterator.hasNext()) {
	    // map.get(iterator.next()).sort();
	    // }
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {

	    if (isOutput == true) {
		TIntHashSet theSet = new TIntHashSet();
		TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();

		int selected_node_id = 0;

		if (key.getNode1Degree() < key.getNode2Degree()) {
		    selected_node_id = 1;
		} else if (key.getNode1Degree() == key.getNode2Degree()) {
		    if (key.getNode1() < key.getNode2()) {
			selected_node_id = 1;
		    } else {
			selected_node_id = 2;
		    }
		} else {
		    selected_node_id = 2;
		}

		int selected_node = (selected_node_id == 1 ? key.getNode1() : key.getNode2());
		int not_selected_node = (selected_node_id == 1 ? key.getNode2() : key.getNode1());

		TIntArrayList theList = map.get(selected_node);
		theSet.addAll(map.get(not_selected_node));

		for (int j = 0; j < theList.size(); ++j) {
		    int tpKey = theList.get(j);
		    if (map.containsKey(tpKey)) {
			TIntArrayList intersectList = map.get(tpKey);

			for (int i = 0; i < intersectList.size(); ++i) {
			    int node = intersectList.get(i);

			    if (tp.containsKey(node)) {
				tp.get(node).add(tpKey);
			    } else {
				if (theSet.contains(node)) {
				    TIntArrayList aList = new TIntArrayList();
				    aList.add(tpKey);
				    tp.put(node, aList);
				}
			    }
			}
		    }
		}

		TIntIterator iterator = tp.keySet().iterator();
		long counter = 0;
		while (iterator.hasNext()) {
		    int theKey = iterator.next();

		    if (theKey != selected_node) {
			TIntArrayList theValue = tp.get(theKey);

			TwoIntArray outputValue = new TwoIntArray(value.size, theValue.size());
			IntArray outputKey = new IntArray(3);
			outputKey.setNode(0, key.getNode1());
			outputKey.setNode(1, key.getNode2());
			outputKey.setNode(2, theKey);
			outputValue.nodeArray1 = value.nodeArray;
			outputValue.nodeArray2 = theValue.toArray();

			context.write(outputKey, outputValue);
		    }

		}
	    } else {
		TIntHashSet theSet = new TIntHashSet();
		TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();

		int selected_node_id = 0;

		if (key.getNode1Degree() < key.getNode2Degree()) {
		    selected_node_id = 1;
		} else if (key.getNode1Degree() == key.getNode2Degree()) {
		    if (key.getNode1() < key.getNode2()) {
			selected_node_id = 1;
		    } else {
			selected_node_id = 2;
		    }
		} else {
		    selected_node_id = 2;
		}

		int selected_node = (selected_node_id == 1 ? key.getNode1() : key.getNode2());
		int not_selected_node = (selected_node_id == 1 ? key.getNode2() : key.getNode1());

		TIntArrayList theList = map.get(selected_node);
		theSet.addAll(map.get(not_selected_node));

		for (int j = 0; j < theList.size(); ++j) {
		    int tpKey = theList.get(j);
		    if (map.containsKey(tpKey)) {
			TIntArrayList intersectList = map.get(tpKey);

			for (int i = 0; i < intersectList.size(); ++i) {
			    int node = intersectList.get(i);

			    if (tp.containsKey(node)) {
				tp.get(node).add(tpKey);
			    } else {
				if (theSet.contains(node)) {
				    TIntArrayList aList = new TIntArrayList();
				    aList.add(tpKey);
				    tp.put(node, aList);
				}
			    }
			}
		    }
		}

		TIntIterator iterator = tp.keySet().iterator();
		long counter = 0;
		while (iterator.hasNext()) {
		    int theKey = iterator.next();

		    if (theKey != selected_node) {
			TIntArrayList theValue = tp.get(theKey);
			for (int k = 0; k < value.size; k++) {
			    if (theKey != value.getNode(k)) {
				for (int i = 0; i < theValue.size(); i++) {
				    if (theValue.get(i) != not_selected_node && theValue.get(i) != value.getNode(k)) {
					++counter;
				    }
				}
			    }
			}
			context.getCounter("test", "core_size").increment(1);
			context.getCounter("test", "list").increment(value.size + theValue.size());
			context.getCounter("test", "pattern_size").increment(counter);
		    }
		}
	    }
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	for (int i = 0; i < 240; ++i) {
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(args[1] + "/part-r-00" + appendNumberToLengthThree(i)), getConf());
	}

	conf.set("mapreduce.map.memory.mb", "4000");
	conf.set("mapreduce.map.java.opts", "-Xmx4000M");
	conf.set("test.line", args[1]);

	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("HouseO");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setOutputKeyClass(IntArray.class);
	job.setOutputValueClass(TwoIntArray.class);
	job.setMapOutputKeyClass(IntArray.class);
	job.setMapOutputValueClass(IntArray.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

	job.setNumReduceTasks(0);

	job.setMapperClass(HouseOMapper.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	// conf.setProfileEnabled(true);
	long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
					     // give any value)
	conf.setLong("mapred.task.timeout", milliSeconds);
	conf.setNumMapTasks(240);
	ToolRunner.run(conf, new HouseSO(), args);

	endTime = System.currentTimeMillis();
	log.info("[HouseO] Time elapsed: " + (endTime - startTime) / 1000 + "s");

    }

}

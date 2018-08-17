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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;




/**
 * <h1>
 * Count SolarSquare
 * </h1>
 * <ul>
 * 	<li>Pattern name: SolarSquare </li>
 * 	<li>Pattern: (0,1),(0,2),(0,3),(0,4),(1,5),(1,2),(1,4),(2,3),(3,4)  </li>
 * 	<li>Core: (0,1,3)</li>
 * 	<li>Non Core Node: [2,4,5]</li>
 * </ul>
 * @author zhanghao
 */
public class SolarSquarePlus extends Configured implements Tool {

    private static Logger log = Logger.getLogger(SolarSquarePlus.class);

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
		int p = 1;
		for (int i = 0; i < p; i++) {
			conf.setInt("test.p", p);
			conf.setInt("test.offset", i);
			ToolRunner.run(conf, new SolarSquarePlus(), args);
		}

		endTime = System.currentTimeMillis();
		log.info("[solar house] Time elapsed: " + (endTime - startTime) / 1000 + "s");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		// create seed file
		for (int i = 0; i < 240; ++i) {
			// DistributedCache.addLocalFiles(getConf(), aString + iString);
			org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(args[1] + "/part-r-00" + appendNumberToLengthThree(i)), getConf());
		}

		int memory_size = conf.getInt("test.memory", 4000);
		int opts_size = (int) (memory_size * 0.8);
		String memory_opts = "-Xmx" + opts_size + "M";

		conf.setInt("mapreduce.map.memory.mb", memory_size);
		conf.set("mapreduce.map.java.opts", memory_opts);
		conf.setInt("mapreduce.reduce.memory.mb", memory_size);
		conf.set("mapreduce.reduce.java.opts", memory_opts);
		conf.set("test.line", args[1]);

		Job job = new Job(conf);

		job.setJarByClass(getClass());
		job.setJobName("SolarSquarePlus");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		// job.setOutputKeyClass(NullWritable.class);
		// job.setOutputValueClass(NullWritable.class);
		// job.setMapOutputKeyClass(NullWritable.class);
		// job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);

		job.setMapperClass(SolarSquarePlusOMapper.class);

		boolean success = job.waitForCompletion(true);
		return 0;
	}

	public static class SolarSquarePlusOMapper extends Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable> {
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);

	private final TIntObjectHashMap<TIntArrayList> map = new TIntObjectHashMap<TIntArrayList>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
		Integer NIBFp = conf.getInt("test.p", 1);
		Integer NIBFoffset = conf.getInt("test.offset", 0);

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    p = NIBFp;
	    offset = NIBFoffset;

	    SequenceFile.Reader reader = null;

	    ArrayList<Integer> shuffleList = new ArrayList<Integer>();
	    for (int i = 0; i < 240; ++i) {

		if ((i % p) == offset) {
		    shuffleList.add(i);
		}
	    }

	    Collections.shuffle(shuffleList, new Random(System.currentTimeMillis()));

	    for (int k = 0; k < shuffleList.size(); k++) {
		reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[shuffleList.get(k)], conf);
		OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

		long position = reader.getPosition();
		while (reader.next(key, value)) {
		    String syncSeen = reader.syncSeen() ? "*" : "";
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
	}

		@Override
		protected void map(TwoNodeKey key, IntDegreeArray value,
						   Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			// left node
			{
				TIntHashSet theSet = new TIntHashSet();
				TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();

				TIntArrayList listToAddSet = map.get(key.node1);
				theSet.addAll(listToAddSet);
				for (int j = 0; j < value.size; ++j) {
					int tpKey = value.getNode(j);
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
					if (theKey != key.getNode1() && theKey != key.getNode2()) {
						TIntArrayList theValue = tp.get(theKey);
						long list_size_2 = theValue.size();
						long list_size_1 = map.get(theKey).size();

//			counter += (list_size_1 - 3) * (list_size_2 * (list_size_2 - 1) / 2);

						if (map.get(theKey).contains(key.node2)) {
							counter += (list_size_1 - 4) * (list_size_2 * (list_size_2 - 1) / 2);
						} else {
							counter += (list_size_1 - 3) * (list_size_2 * (list_size_2 - 1) / 2);
						}

					}
				}

				context.getCounter("test", "graph2").increment(counter);
			}

	    // right node
			{
				TIntHashSet theSet = new TIntHashSet();
				TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();

				TIntArrayList listToAddSet = map.get(key.node2);
				theSet.addAll(listToAddSet);
				for (int j = 0; j < value.size; ++j) {
					int tpKey = value.getNode(j);
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
					if (theKey != key.getNode1() && theKey != key.getNode2()) {
						TIntArrayList theValue = tp.get(theKey);


						long list_size_2 = theValue.size();
						long list_size_1 = map.get(theKey).size();

//			counter += (list_size_1-3) * (list_size_2 * (list_size_2 - 1) / 2);

						if (map.get(theKey).contains(key.node1)) {
							counter += (list_size_1 - 4) * (list_size_2 * (list_size_2 - 1) / 2);
						} else {
							counter += (list_size_1 - 3) * (list_size_2 * (list_size_2 - 1) / 2);
						}

						// this counting is just a approximation
					}
				}

				context.getCounter("test", "graph2").increment(counter);
			}

	}
    }

}

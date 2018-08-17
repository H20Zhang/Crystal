package sAlg;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.IntArray;
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
 * Output Square
 * </h1>
 * <ul>
 * 	<li>Pattern name: Square </li>
 * 	<li>Pattern: (0,1),(1,2),(2,3),(3,0)  </li>
 * 	<li>Core: (0,2)</li>
 * 	<li>Non Core Node: [1,3]</li>
 * </ul>
 * @author zhanghao
 */

public class SquareO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(SquareO.class);

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
        GenericOptionsParser paraser = new GenericOptionsParser(conf, args);
        // conf.setProfileEnabled(true);
        long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
        // give any value)
        conf.setLong("mapred.task.timeout", milliSeconds);

        conf.setNumMapTasks(240);
		int p = conf.getInt("test.p", 1);
		conf.setInt("test.p", p);
		for (int i = 0; i < p; i++) {
            conf.setInt("test.offset", i);
            ToolRunner.run(conf, new SquareO(), args);
        }

        endTime = System.currentTimeMillis();
        log.info("[square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // create seed file
        for (int i = 0; i < 240; ++i) {
            // DistributedCache.addLocalFiles(getConf(), aString + iString);
            org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(args[1] + "/part-r-00" + appendNumberToLengthThree(i)), getConf());
        }

        conf.set("mapreduce.map.memory.mb", "4000");
        conf.set("mapreduce.map.java.opts", "-Xmx4000M");
        conf.set("test.line", args[1]);

        Job job = new Job(conf);

        job.setJarByClass(getClass());
        job.setJobName("SquareO");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(TwoNodeKey.class);
        job.setOutputValueClass(IntArray.class);
        job.setMapOutputKeyClass(TwoNodeKey.class);
        job.setMapOutputValueClass(IntArray.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setNumReduceTasks(0);

        job.setMapperClass(SquareOMapper.class);

        boolean success = job.waitForCompletion(true);
        return 0;
    }

    public static class SquareOMapper extends Mapper<OneNodeKey, IntDegreeArray, TwoNodeKey, IntArray> {
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private Boolean isOutput = false;

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
	    Collections.shuffle(shuffleList, new Random(System.currentTimeMillis()));

	    for (int k = 0; k < shuffleList.size(); k++) {
		// FileInputStream in = new FileInputStream(new
		// File(allPaths[k].toString()));
		// DataInputStream input = new DataInputStream(in);
		// FSDataInputStream fsin = new FSDataInputStream(input);
		// SequenceFile.Reader.Option file =
		// SequenceFile.Reader.file(allPaths[k]);
		// SequenceFile.Reader.Option stream =
		// SequenceFile.Reader.stream(fsin);
		// reader = new SequenceFile.Reader(conf, file, stream);

		reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[shuffleList.get(k)], conf);
		// reader = new SequenceFile.Reader(fs, new
		// Path(path+appendNumberToLengthThree(shuffleList.get(k))),
		// conf);
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
	}

	@Override
	protected void map(OneNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {

	    if (isOutput == false) {
		TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();
		for (int j = 0; j < value.size; ++j) {
		    int tpKey = value.getNode(j);
		    if (map.containsKey(tpKey)) {
			TIntArrayList intersectList = map.get(tpKey);

			for (int i = 0; i < intersectList.size(); ++i) {
			    int node = intersectList.get(i);
			    if (tp.containsKey(node)) {
				tp.get(node).add(tpKey);
			    } else {
				TIntArrayList aList = new TIntArrayList();
				aList.add(tpKey);
				tp.put(node, aList);
			    }
			}
		    }
		}

		TIntIterator iterator = tp.keySet().iterator();

		while (iterator.hasNext()) {
		    int theKey = iterator.next();
		    if (theKey < key.getNode()) {
			int counter = 0;
			TIntArrayList theValue = tp.get(theKey);
			for (int i = 0; i < theValue.size(); i++) {
			    if (theValue.get(i) > theKey) {
				++counter;
			    }
			}

			long l1 = counter;
			long l2 = l1 * (l1 - 1);
			long l3 = l2 / 2;

			context.getCounter("test", "core_size").increment(1);
			context.getCounter("test", "list_size").increment(l1);
			context.getCounter("test", "pattern_size").increment(l3);
		    }
		}
	    } else {
		TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();
		for (int j = 0; j < value.size; ++j) {
		    int tpKey = value.getNode(j);
		    if (map.containsKey(tpKey)) {
			TIntArrayList intersectList = map.get(tpKey);

			for (int i = 0; i < intersectList.size(); ++i) {
			    int node = intersectList.get(i);
			    if (tp.containsKey(node)) {
				tp.get(node).add(tpKey);
			    } else {
				TIntArrayList aList = new TIntArrayList();
				aList.add(tpKey);
				tp.put(node, aList);
			    }
			}
		    }
		}

		TIntIterator iterator = tp.keySet().iterator();

		while (iterator.hasNext()) {
		    int theKey = iterator.next();
		    if (theKey < key.getNode()) {
			TIntArrayList theValue = tp.get(theKey);
			IntArray aValue = new IntArray(theValue.size());

			int counter = 0;

			for (int i = 0; i < theValue.size(); i++) {
			    if (theValue.get(i) > theKey) {
				aValue.setNode(counter++, theValue.get(i));
			    }
			}

			aValue.size = counter;

			context.write(new TwoNodeKey(key.getNode(), theKey, 0, 0), aValue);

		    }
		}
	    }
	}
    }

}

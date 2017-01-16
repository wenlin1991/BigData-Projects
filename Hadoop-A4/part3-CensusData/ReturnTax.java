/**
 *  CS696 Big data tools
 *  Assignment #4: Question 3 part a
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReturnTax {


    /**
     *  CompositeKey class
     *  Create composite key to hold the word pair: word1 and word2
     */
	public static class CompositeKey 
	    implements WritableComparable<CompositeKey> {

	    public String state;
	    public String category;

	    public CompositeKey() {}

	    public CompositeKey(String state, String category) {
	    	super();
	    	this.set(state, category);
	    }

	    public void set(String state, String category) {
	    	this.state = state;
	    	this.category = category;
	    }

	    public void write(DataOutput out) throws IOException {
	    	out.writeUTF(state);
	    	out.writeUTF(category);
	    }

	    public void readFields(DataInput in) throws IOException {
	    	state = in.readUTF();
	    	category = in.readUTF();
	    }

	    public int compareTo(CompositeKey key) {
	    	int stateCmp = state.compareTo(key.state);
	    	if (stateCmp != 0) {
	    		return stateCmp;
	    	} else {
	    		int categoryCmp = category.compareTo(key.category);
	    		return categoryCmp;
	    	}
	    } 

	    public String toString() {
	    	return (state + " " + category);
	    }
	}

    /**
     *  Mapper class
     */
	public static class TaxSumMapper
	    extends Mapper<Object, Text, CompositeKey, IntWritable> {
	    private CompositeKey keyPair = new CompositeKey();

	    public void map(Object key, Text value, Context context)
	        throws IOException, InterruptedException {
	        String dataString = value.toString();
	        String[] dataArray = dataString.split(",");
	        String state = dataArray[1];
	        String category = dataArray[3];
	        int returnAmount = (int) Float.parseFloat(dataArray[4]);
	        keyPair.set(state, category);
	        context.write(keyPair, new IntWritable(returnAmount));
	    }
	}

    
    /**
     *  Reducer class for sum job
     */ 
	public static class TaxSumReducer 
	    extends Reducer<Text, IntWritable, CompositeKey, IntWritable> {
	    private IntWritable totalAmount = new IntWritable();

	    public void reduce(CompositeKey key, Iterable<IntWritable> values,
	    	Context context) 
	        throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	        	sum += val.get();
	        }
	        totalAmount.set(sum);
	        context.write(key, totalAmount);
	    }
	}

    /**
     *  Mapper class to sort the output
     */ 
	public static class SortMapper 
	    extends Mapper<Object, Text, CompositeKey, IntWritable> {
	    private CompositeKey stateCategory = new CompositeKey();

	    public void map(Object key, Text value, Context context) 
	        throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String state = itr.nextToken();
            String category = itr.nextToken();
            stateCategory.set(state, category);
            int num = Integer.parseInt(itr.nextToken());
            context.write(stateCategory, new IntWritable(num));    
	    } 
	}

    /**
     *  Comparator class to sort the output
     */ 
	public static class KeyPairComparator extends WritableComparator {
		public KeyPairComparator() {
			super(CompositeKey.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override 
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;
			return key1.compareTo(key2);
		}
	}

    /**
     *  Main driver
     */ 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job taxSum = Job.getInstance(conf, "tax sum");
		taxSum.setJarByClass(ReturnTax.class);
		taxSum.setMapperClass(TaxSumMapper.class);
		taxSum.setCombinerClass(TaxSumReducer.class);
		taxSum.setReducerClass(TaxSumReducer.class);
		taxSum.setOutputKeyClass(CompositeKey.class);
		taxSum.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(taxSum, new Path(args[0]));
		FileOutputFormat.setOutputPath(taxSum, new Path(args[1] + "/sum"));
		taxSum.waitForCompletion(true);

		Configuration sortConf = new Configuration();
		Job sort = Job.getInstance(sortConf, "sort tax sum");
		sort.setJarByClass(ReturnTax.class);
		sort.setMapperClass(SortMapper.class);
		sort.setOutputKeyClass(CompositeKey.class);
		sort.setOutputValueClass(IntWritable.class);
		sort.setSortComparatorClass(KeyPairComparator.class);
		FileInputFormat.addInputPath(sort, new Path(args[1] + "/sum"));
		FileOutputFormat.setOutputPath(sort, new Path(args[1] + "/sort"));
		System.exit(sort.waitForCompletion(true) ? 0 : 1);
	}
}








































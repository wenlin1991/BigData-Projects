/**
 *  CS696 Big data tools
 *  Assignment #4: Question 2 part b
 *  Author: Wen Lin
 *  Prof. Whitney
 */



import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput; 
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairCount {

    /**
     *  CompositeKey class
     *  Create composite key to hold the word pair: word1 and word2
     */
	public static class CompositeKey implements WritableComparable<CompositeKey> {
		public String firstWord;
		public String secondWord;

		public CompositeKey() {}

		public CompositeKey(String firstWord, String secondWord) {
			super();
			this.set(firstWord, secondWord);
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(firstWord);
			out.writeUTF(secondWord);
		}

		public void set(String firstWord, String secondWord) {
			this.firstWord = firstWord;
			this.secondWord = secondWord;
		}

		public void readFields(DataInput in) throws IOException {
			firstWord = in.readUTF();
			secondWord = in.readUTF();
		}

        @Override
		public int compareTo(CompositeKey o) {
			int firstCmp = firstWord.compareTo(o.firstWord);
			if (firstCmp != 0) {
				return firstCmp;
			} else {
				int secondCmp = secondWord.compareTo(o.secondWord);
				return secondCmp;
			}
		}

		public String toString() {
			return (firstWord + " " + secondWord);
		}
	}

    /**
     *  Mapper class
     */
    public static class TokenizerMapper
        extends Mapper<Object, Text, CompositeKey, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
	    private CompositeKey wordPair = new CompositeKey();

	    public void map(Object key, Text value, Context context
			      ) throws IOException, InterruptedException {
		    String lineText = value.toString();
		    String[] lineWord = lineText.split("\\s+");
		    int lineLength = lineWord.length;
		    String endLine = lineWord[lineLength-1];
		    for (int i = 1; i < lineLength; i++) {
		  	    wordPair.set(lineWord[i-1], lineWord[i]);
		  	    context.write(wordPair, one);
		    }
        }
    }


    /**
     *  Mapper class to sort the output
     */    
    public static class SortMapper
        extends Mapper<Object, Text, LongWritable, CompositeKey>{

	    private CompositeKey keyPair = new CompositeKey();

        public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
  	        StringTokenizer itr = new StringTokenizer(value.toString());	
	      	String word1 = itr.nextToken();
	      	String word2 = itr.nextToken();
	      	keyPair.set(word1, word2);
	      	int num = Integer.parseInt(itr.nextToken());
		    context.write(new LongWritable(num), keyPair);
        }
    }

    /**
     *  Comparator class to sort the output
     */ 
	public static class DescendingIntComparator extends WritableComparator {

		public DescendingIntComparator() { 
			super(LongWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			LongWritable key1 = (LongWritable) w1; 
			LongWritable key2 = (LongWritable) w2; 
			return -1 * key1.compareTo(key2);
		} 
	}

    /**
     *  Reduce class for sum job
     */ 
    public static class IntSumReducer
        extends Reducer<CompositeKey,LongWritable,CompositeKey,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(CompositeKey key, Iterable<LongWritable> values,
                           Context context) 
            throws IOException, InterruptedException {
	        int sum = 0;
	        for (LongWritable val : values) {
	        sum += val.get();
	    }
	        result.set(sum);
	        context.write(key, result);
        }
    }

    /**
     *  Comparator class to sort the output
     */ 
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(PairCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/count"));
		job.waitForCompletion(true);

	    Configuration sortConf = new Configuration();
		Job job2 = Job.getInstance(sortConf, "sort output");
		job2.setJarByClass(PairCount.class);
		job2.setMapperClass(SortMapper.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(CompositeKey.class);
		job2.setSortComparatorClass(DescendingIntComparator.class);

		FileInputFormat.addInputPath(job2, new Path(args[1] + "/count"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/sort"));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

















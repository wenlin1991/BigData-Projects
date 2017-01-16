/**
 *  CS696 Big data tools
 *  Assignment #4: Question 2 part a
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import java.io.IOException;
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

public class WordCount {

    /**
     *  Mapper class
     */
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);	    
        private Text word = new Text();

	    public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		    StringTokenizer itr = new StringTokenizer(value.toString());
		    while (itr.hasMoreTokens()) {
		        word.set(itr.nextToken());
		        context.write(word, one);
		    }
        }
    }

    /**
     *  Mapper class to sort the output
     */ 
    public static class SortMapper
        extends Mapper<Object, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
	        StringTokenizer itr = new StringTokenizer(value.toString());
	        try {		
	      	    word = new Text(itr.nextToken());
	      	    int num = Integer.parseInt(itr.nextToken());
		        context.write(new IntWritable(num), word);
		    } catch (IOException e){
		  	    System.out.println("Error in Mapper sort");
		    }
        }
    }

    /**
     *  Comparator class to sort the output
     */ 
	public static class DescendingIntComparator extends WritableComparator {
		public DescendingIntComparator() { 
			super(IntWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1; 
			IntWritable key2 = (IntWritable) w2; 
			return -1 * key1.compareTo(key2);
		} 
	}

    /**
     *  Reducer class for sum job
     */ 
    public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
			) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	    result.set(sum);
	    context.write(key, result);
        }
    }

    /**
     *  Main driver
     */ 
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/count"));
		job.waitForCompletion(true);

	    Configuration sortConf = new Configuration();
		Job job2 = Job.getInstance(sortConf, "sort output");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(SortMapper.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setSortComparatorClass(DescendingIntComparator.class);

		FileInputFormat.addInputPath(job2, new Path(args[1] + "/count"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/sort"));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
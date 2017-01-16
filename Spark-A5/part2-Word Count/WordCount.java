/**
 *  CS696 Big data tools
 *  Assignment #5: Question 2 part b
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final String REGEX1 = "'s|'|ly|ing|ness|ed|--|-|,|;|_|[()]|[!]|[?]|[.]|[:]|$";
	private static final String REGEX2 = "\"|\"";
	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			System.err.println("Usage: PairCount <inputPath> <outputPath>");
			System.exit(1);
		}
		String inputPath = args[0];
		String outputPath = args[1];

        // Read in file and parse words 
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> lines = sc.textFile(inputPath);
		JavaRDD<String> words = lines.flatMap(line -> 
			Arrays.asList(line.replaceAll(REGEX1, "").replaceAll(REGEX2, "").toLowerCase().split("\\s+")).iterator());

		// Count words and save output into file
		JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((x, y) -> x + y);
		JavaPairRDD<Integer, String> swapped = counts.mapToPair(pair -> pair.swap());
		JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);
		
		sorted.coalesce(1).saveAsTextFile(outputPath);
		sc.stop();
	}
}
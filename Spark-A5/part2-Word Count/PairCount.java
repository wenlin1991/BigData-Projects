/**
 *  CS696 Big data tools
 *  Assignment #5: Question 2 part b
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;

public class PairCount {

	// Regex for parsing words 
	private static final Pattern SPACE = Pattern.compile(" ");
	private static final String REGEX1 = "'s|'|ly|ing|ness|ed|--|-|,|;|_|[()]|[!]|[?]|[.]|[:]|$";
	private static final String REGEX2 = "\"|\"";

	// Match the last word of a line to the first word of next line
	private static String end = "";  
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: PairCount <inputPath> <outputPath>");
			System.exit(1);
		}
		String inputPath = args[0];
		String outputPath = args[1];

		// Read in and parse words 
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> lines = sc.textFile(inputPath, 1);
		JavaRDD<String> pairs = lines.flatMap(line -> {
			    String[] words = line.replaceAll(REGEX1, "").replaceAll(REGEX2, "").toLowerCase().split("\\s+");
			    List<String> results = new ArrayList<>();

			    // Match the last word of each line to the first word of next line
			    if (!end.isEmpty() && words.length != 0) {
			    	results.add(end + " " + words[0]); 
			    	end = words[words.length-1];
			    } else if (words.length != 0) {
			    	end = words[words.length-1];
			    }
			    for (int i=1; i < words.length; i++) {
				    results.add(words[i-1] + " " + words[i]);
			    }
			    return results.iterator();
		    }
		);

		// Count pairs and sort by the frequency
		JavaPairRDD<String, Integer> ones = pairs.mapToPair(pair -> new Tuple2<>(pair, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((x, y) -> x + y);
		JavaPairRDD<Integer, String> swapped = counts.mapToPair(pair -> pair.swap());
		JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);
		sorted.coalesce(1).saveAsTextFile(outputPath);
		sc.stop();
	}
}
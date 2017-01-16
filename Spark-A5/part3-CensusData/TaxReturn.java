/**
 *  CS696 Big data tools
 *  Assignment #5: Question 3
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;

public class TaxReturn {

	private static final Pattern COMMA = Pattern.compile(",");
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: TaxReturn <inputPath> <outputPath>");
			System.exit(1);
		}
		String inputPath = args[0];
		String outputPath = args[1];

		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> lines = sc.textFile(inputPath);

        // Get key pair: key has two components (state, category)
		JavaRDD<String> stateCategory = lines.flatMap(line -> {
			String[] data = COMMA.split(line);
			List<String> results = new ArrayList<>();

			String state = data[1];
			String category = data[3];
			results.add(state + " " + category);
			return results.iterator();
		});

		// Get the amount for each key pair
		JavaRDD<Integer> amount = lines.flatMap(line -> {
			String[] data = COMMA.split(line);
			List<Integer> results = new ArrayList<>();

			int returnAmount = (int) Float.parseFloat(data[4]);
			results.add(returnAmount);
			return results.iterator();
		});

		// Zip keyPair (stateCategory) and amount into a JavaPairRDD, then sort by key
		JavaPairRDD<String, Integer> stateCategoryAmount = stateCategory.zip(amount);
		stateCategory.collect();
		JavaPairRDD<String, Integer> counts = stateCategoryAmount.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Integer> sorted = counts.sortByKey();

		// Make the output in one file 
		sorted.coalesce(1).saveAsTextFile(outputPath);
		sc.stop();
	}
}
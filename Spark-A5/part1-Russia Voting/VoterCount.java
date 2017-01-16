/**
 *  CS696 Big data tools
 *  Assignment #5: Question 1 part a
 *  Author: Wen Lin
 *  Prof. Whitney
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import java.util.Arrays;
import java.util.regex.Pattern;

public class VoterCount {

	private static final Pattern COMMA = Pattern.compile(",");
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: VoterCount <inputPath> <outputPath>");
			System.exit(1);
		}
		String inputPath = args[0];
		String outputPath = args[1];

		// Read in file 
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> lines = sc.textFile(inputPath);

        // Get key (district) value (voter number) pair
		JavaRDD<String> district = lines.flatMap(line -> Arrays.asList(COMMA.split(line)[0]).iterator());
		JavaRDD<Integer> count = lines.flatMap(line -> Arrays.asList(Integer.parseInt(COMMA.split(line)[3])).iterator());
		JavaPairRDD<String, Integer> tempPair = district.zip(count);
		tempPair.collect();

		// Reduce add up voters in the same district 
		JavaPairRDD<String, Integer> districtVoter = tempPair.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Integer> sorted = districtVoter.sortByKey();
		sorted.coalesce(1).saveAsTextFile(outputPath);
		sc.stop();
	}
}
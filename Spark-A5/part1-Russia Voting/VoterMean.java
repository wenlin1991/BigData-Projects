/**
 *  CS696 Big data tools
 *  Assignment #5: Question 1 part b
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

public class VoterMean {

    private static final Pattern COMMA = Pattern.compile(",");
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: VoterMean <inputPath> <outputPath>");
			System.exit(1);
		}
		String inputPath = args[0];
		String outputPath = args[1];

        // Read in file and parse data
		JavaSparkContext sc = new JavaSparkContext();
		String regex = "[()]";
		JavaRDD<Integer> districtVoter = sc.textFile(inputPath)
		    .map(line -> Integer.parseInt(COMMA.split(line.replaceAll(regex, ""))[1]));
		int totalVoter = districtVoter.reduce((x, y) -> x + y);
		long count = districtVoter.count();
		Double votermean = ((double) totalVoter) / count;
		JavaRDD<Double> mean = sc.parallelize(Arrays.asList(votermean));

		System.out.println("==========Output goes here==========");
		System.out.println("The mean voter is: " + votermean);
		System.out.println("==========Output finish=============");
		mean.coalesce(1).saveAsTextFile(outputPath);
		sc.stop();
	}
}
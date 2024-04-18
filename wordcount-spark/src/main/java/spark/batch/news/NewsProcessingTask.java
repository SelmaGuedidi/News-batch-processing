package spark.batch.news;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.batch.tp21.WordCountTask;

import java.util.Arrays;
import java.util.List;
import com.google.common.base.Preconditions;

public class NewsProcessingTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewsProcessingTask.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new NewsProcessingTask().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
         String master = "local[*]";
          SparkConf conf = new SparkConf()
                  .setAppName(WordCountTask.class.getName())
                  .setMaster(master);
          JavaSparkContext sc = new JavaSparkContext(conf);
        

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        // Drop columns: title, subject, date
        JavaRDD<String> cleanedTextFile = textFile.map(line -> {
            String[] parts = line.split(","); // Assuming CSV format
            // Selecting only the 'text' column (assuming it's the last column)
            if (parts.length > 1) {
                // Selecting only the 'text' column (assuming it's the last column)
                return parts[1];
            } else {
                // Handle lines with insufficient data, e.g., log a warning
                LOGGER.warn("Line does not have expected number of columns: {}", line);
                // Return a default value (e.g., empty string) or handle as appropriate
                return "";
            }
        });

        // Define UDF for text preprocessing
        JavaRDD<String> preprocessedTextFile = cleanedTextFile.map(sentence -> {
            // Perform preprocessing: removing URLs and converting to lowercase
            return sentence.replaceAll("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]", "")
                    .toLowerCase();
        });

       
        // Save word counts to output directory
        preprocessedTextFile.saveAsTextFile(outputDir);

        // Stop SparkContext
        sc.stop();
    }
}

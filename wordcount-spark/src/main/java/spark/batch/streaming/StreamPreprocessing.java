package spark.batch.streaming;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import io.netty.handler.timeout.TimeoutException;

import java.util.Arrays;
import java.util.regex.Pattern;

public class StreamPreprocessing {  
        public static void main(String[] args) throws StreamingQueryException, TimeoutException, java.util.concurrent.TimeoutException {
                SparkSession spark = SparkSession
                    .builder()
                    .appName("NetworkWordCount")
                    .master("local[*]")
                    .getOrCreate();
        
                // Create DataFrame representing the stream of input lines from connection to localhost:9999
                Dataset<String> lines = spark
                    .readStream()
                    .format("socket")
                    .option("host", "localhost")
                    .option("port", 9999)
                    .load()
                    .as(Encoders.STRING());
        
                // Apply transformations to lowercase and remove URLs
                Dataset<String> processedLines = lines.map((MapFunction ) line -> {
                        // Convert to lowercase
                        String lowerCase = ((String) line).toLowerCase();
                        // Remove URLs
                        String noUrlLine = removeUrls(lowerCase);
                        return noUrlLine;
                    }, Encoders.STRING());
        
                // Start running the query that prints the processed lines to the console
                StreamingQuery query = processedLines.writeStream()
                    .outputMode("append")
                    .format("console")
                    .trigger(Trigger.ProcessingTime("1 second"))
                    .start();
        
                query.awaitTermination();
            }
        
            // Function to remove URLs from a string
            private static final Pattern URL_REGEX = Pattern.compile(
                "(?i)\\b((?:https?://|www\\d{0,3}[.]|[a-z0-9.\\-]+[.][a-z]{2,4}/)(?:[^\\s()<>]+|\\(([^\\s()<>]+|(\\([^\\s()<>]+\\)))*\\))+(?:\\(([^\\s()<>]+|(\\([^\\s()<>]+\\)))*\\)|[^\\s`!()\\[\\]{};:'\".,<>?«»“”‘’]))"
            );
           
            private static String removeUrls(String text) {
                return URL_REGEX.matcher(text).replaceAll("");
            }}
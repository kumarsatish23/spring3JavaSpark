package in.vanna.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.multipart.MultipartFile;
import scala.Tuple2;

@Service
public class SparkFileService {

	public Map<String, Object> processData(MultipartFile file) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Line Count").setMaster("local[*]");
		try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
			File tempFile;
			Map<String, Object> response = new HashMap<>();
			try {
				tempFile = Files.createTempFile("temp", file.getOriginalFilename()).toFile();
				FileCopyUtils.copy(file.getBytes(), tempFile);
			} catch (IOException e) {
				response.put("Error", e);
				return response;
			}
			JavaRDD<String> lines = sparkContext.textFile(tempFile.getAbsolutePath());
			response.put("Number Of Lines", lines.count());
			response.put("content", lines.take(10));
			final var filteredWords = sparkContext.textFile(tempFile.getAbsolutePath())
					.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
					.flatMap(line -> List.of(line.split("\\s")).iterator())
					.filter(word -> ((word != null) && (word.trim().length() > 0)));
			final var counts = filteredWords.mapToPair(word -> new Tuple2<>(word, 1L)).reduceByKey(Long::sum);
			response.put("repeting linses",
					counts.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).sortByKey(false).take(10));

			sparkContext.stop();
			tempFile.delete();
			return response;
		}
	}

	public Map<String, Long> bar(MultipartFile file) {
		SparkConf conf = new SparkConf().setAppName("Line Count").setMaster("local[*]");
		try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
			File tempFile;
			Map<String,Long> response = new HashMap<>();
			try {
				tempFile = Files.createTempFile("temp", file.getOriginalFilename()).toFile();
				FileCopyUtils.copy(file.getBytes(), tempFile);
			} catch (IOException e) {
				response.put( "Error",0L);
				return response;
			}
			final var filteredWords = sparkContext.textFile(tempFile.getAbsolutePath())
					.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
					.flatMap(line -> List.of(line.split("\\s")).iterator())
					.filter(word -> ((word != null) && (word.trim().length() > 0)));
			final var counts = filteredWords.mapToPair(word -> new Tuple2<>(word, 1L)).reduceByKey(Long::sum);
			Map<String, Long> collectAsMap = counts.mapToPair(tuple -> new Tuple2<>(tuple._1 , tuple._2 )).sortByKey().collectAsMap();
			sparkContext.stop();
			tempFile.delete();
			return  collectAsMap;
		}
	}
}

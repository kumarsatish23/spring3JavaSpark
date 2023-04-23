package in.vanna.service;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class SparkFileService {

    public long processData(MultipartFile file) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Data Processor").setMaster("local[*]");
        try (var sparkContext = new JavaSparkContext(conf)) {
			final var myRdd=sparkContext.textFile(file.getResource().getURI().getRawPath());
			
			return myRdd.count();
		}
    }
}

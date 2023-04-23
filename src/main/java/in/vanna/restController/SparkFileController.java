package in.vanna.restController;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import in.vanna.service.SparkFileService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@RestController
public class SparkFileController {

	@Autowired
	SparkFileService fileService;
	
	@PostMapping(value = "/countLine", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Long countLine(@RequestParam("file") MultipartFile file) throws IllegalStateException, IOException {
		
        long response = fileService.processData(file);
        return response;
    }
	
	@PostMapping("/countLines")
    public ResponseEntity<Long> countLines(@RequestParam("file") MultipartFile file) {
        // Initialize Spark
        SparkConf conf = new SparkConf().setAppName("Line Count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a temporary file to store the uploaded file
        File tempFile;
        try {
            tempFile = Files.createTempFile("temp", null).toFile();
            FileCopyUtils.copy(file.getBytes(), tempFile);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        // Load the file
        JavaRDD<String> lines = sc.textFile(tempFile.getAbsolutePath());

        // Count the lines
        long count = lines.count();

        // Stop Spark and delete the temporary file
        sc.stop();
        tempFile.delete();

        // Return the result
        return new ResponseEntity<>(count, HttpStatus.OK);
    }
}

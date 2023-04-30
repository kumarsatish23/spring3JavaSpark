package in.vanna.restController;

import in.vanna.service.SparkFileService;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class SparkFileController {

	@Autowired
	SparkFileService fileService;

	@PostMapping(value = "/countLines", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<Object> countLines(@RequestParam("file") MultipartFile file) throws IOException {
		Map<String, Object> response = fileService.processData(file);
		if (response.containsKey("Error")) {
			return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
		} else
			return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping(value = "/bar", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public Map<String, Long> bar(@RequestParam("file") MultipartFile file) throws IOException {
		return fileService.bar(file);

	}
	@PostMapping(value="/hello")
	public String hello() {
		return "hello world";
	}
}

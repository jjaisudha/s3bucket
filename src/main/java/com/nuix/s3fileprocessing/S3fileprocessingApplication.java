package com.nuix.s3fileprocessing;

import com.nuix.s3fileprocessing.service.S3Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class S3fileprocessingApplication implements CommandLineRunner {

	@Autowired
	S3Service s3Services;

	@Value("${s3.access.key.name}")
	private String key;

	public static void main(String[] args) {
		SpringApplication.run(S3fileprocessingApplication.class, args);

	}
	@Override
	public void run(String... args) throws Exception {


		System.out.println("---------------- START DOWNLOAD FILE ----------------");
		s3Services.downloadFile(key);
		System.out.println("---------------- START SEARCHING INTO FILE ----------------");
		s3Services.filterCsvFile("ellipsis");
		System.out.println("---------------- CONVERT TO PARQUET FILE----------------");
		s3Services.createParquetFile("src/main/resources/filtered/");
		System.out.println("---------------- START UPLOAD FILE ----------------");
		s3Services.uploadFile(key, "src/main/resources/filtered/");

	}

}

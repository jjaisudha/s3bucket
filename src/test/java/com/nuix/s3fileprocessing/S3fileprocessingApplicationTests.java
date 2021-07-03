package com.nuix.s3fileprocessing;

import com.amazonaws.services.s3.AmazonS3;
import com.nuix.s3fileprocessing.service.S3ServiceImpl;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
@Ignore
class S3fileprocessingApplicationTests {

	@Autowired
	private S3ServiceImpl s3Service;
	@Value("${s3.access.key.name}")
	private String key;
	File destDir = new File("src/main/resources/");
	File filteredFilesDir = new File("src/main/resources/filtered");

/*
	@Test
	void contextLoads() {
	}*//*
	@Test
	void contextLoads() {
	}*/

	@Test
	public void downloadTest() {
		s3Service.downloadFile(key);
		File[] files = destDir.listFiles(obj -> obj.isFile() && obj.getName().endsWith(".csv"));
		Assert.assertTrue(files.length>0);
	}

	@Test
	public void testSearchCsv(){
		try {
			s3Service.filterCsvFile("ellipsis");
			File [] filteredFiles = filteredFilesDir.listFiles(obj -> obj.isFile() && obj.getName().endsWith(".csv"));
			Assert.assertTrue(filteredFiles.length>0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

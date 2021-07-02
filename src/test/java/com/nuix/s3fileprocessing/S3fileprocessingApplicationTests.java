package com.nuix.s3fileprocessing;

import com.amazonaws.services.s3.AmazonS3;
import com.nuix.s3fileprocessing.service.S3ServiceImpl;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Ignore
class S3fileprocessingApplicationTests {

	@Autowired
	private S3ServiceImpl s3Service;
	@Value("${s3.access.key.name}")
	private String key;


	@Test
	void contextLoads() {
	}

	@Test
	public void dowanloadTest() {
		s3Service.downloadFile(key);

	}

}

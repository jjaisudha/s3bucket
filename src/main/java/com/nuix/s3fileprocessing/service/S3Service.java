package com.nuix.s3fileprocessing.service;

import java.io.IOException;

public interface S3Service {
    public void downloadFile(String keyName);
    public void filterCsvFile(String filterValue) throws IOException;
    public void uploadFile(String keyName, String uploadFilePath);
}
package com.nuix.s3fileprocessing.service;

import java.io.IOException;

public interface S3Service {
    /**
     *
     * @param keyName
     */
    public void downloadFile(String keyName);

    /**
     *
     * @param filterValue
     * @throws IOException
     */
    public void filterCsvFile(String filterValue) throws IOException;

    /**
     *
     * @param keyName
     * @param uploadFilePath
     */
    public void uploadFile(String keyName, String uploadFilePath);
}
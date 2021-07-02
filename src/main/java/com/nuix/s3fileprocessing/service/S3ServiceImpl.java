package com.nuix.s3fileprocessing.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.*;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;



@Service
public class S3ServiceImpl implements S3Service {

    private final Logger logger = LoggerFactory.getLogger(S3ServiceImpl.class);
    @Autowired
    private AmazonS3 s3client;

    @Value("${s3.bucket}")
    private String bucketName;

    File destDir = new File("src/main/resources/");

    /**
     *
     * @param keyName
     */
    @Override
    public void downloadFile(String keyName) {
        ObjectListing inputFileObjects = null;
        String fileKey = null;
        byte[] buffer = new byte[1024];

        // Get summary information for all objects in the input bucket
        inputFileObjects = s3client.listObjects(bucketName);
        do {
            // Iterate over the list of object summaries
            // Get the object key from each object summary
            for (S3ObjectSummary objectSummary : inputFileObjects.getObjectSummaries()) {
                fileKey = objectSummary.getKey();
                logger.info("downloadFile starts: " + fileKey);

                if (fileKey.endsWith(".zip")) {
                    // Retrieve the object with the specified key from the input bucket
                    S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, fileKey));
                    ZipInputStream zis = new ZipInputStream(s3object.getObjectContent());
                    try {
                        ZipEntry entry = zis.getNextEntry();

                        while(entry != null) {
                            String fileName = entry.getName();
                            File newFile = new File(destDir, entry.getName());
                            FileNameMap fileNameMap = URLConnection.getFileNameMap();
                            String mimeType = fileNameMap.getContentTypeFor(fileName);
                            //String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(fileName)).mimeType();
                            System.out.println("Extracting " + fileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize());
                            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                            int len;
                            while ((len = zis.read(buffer)) > 0) {
                                outputStream.write(buffer, 0, len);
                            }
                            InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
                            if (!entry.isDirectory() && !newFile.exists())
                            {
                                try (FileOutputStream newoutputStream = new FileOutputStream(newFile))
                                {
                                   // BufferedInputStream inputStream = new BufferedInputStream(zipFile.getInputStream(entry));
                                    while (is.available() > 0) {
                                        newoutputStream.write(is.read());
                                    }
                                    is.close();
                                }
                            }

                            /*ObjectMetadata meta = new ObjectMetadata();
                            meta.setContentLength(outputStream.size());
                            meta.setContentType(mimeType);
                            s3client.putObject(bucketName, FilenameUtils.getFullPath(fileKey) + fileName, is, meta);
                            is.close();
                            outputStream.close();*/
                            entry = zis.getNextEntry();
                        }
                        zis.closeEntry();
                        zis.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    logger.info("Content-Type: " + s3object.getObjectMetadata().getContentType());

                }
                inputFileObjects = s3client.listNextBatchOfObjects(inputFileObjects);
            }
        } while (inputFileObjects.isTruncated());

        logger.info("downloadFile ends: " + fileKey);
    }

    /**
     *
     * @param filterValue
     * @throws IOException
     */
    @Override
    public void filterCsvFile(String filterValue) throws IOException {
        logger.info("filterCsvFile starts: " + filterValue);

        File [] files = destDir.listFiles(obj -> obj.isFile() && obj.getName().endsWith(".csv"));
        final char csvDelimeter = ',';

        for(File csvSourceFile:files){
            logger.info("Scanning the file : " + csvSourceFile.getName());
            int lastDotIndex = csvSourceFile.getName().lastIndexOf('.');
            String outputfile = csvSourceFile.getName().substring(0, lastDotIndex ) + "_FilteredFile" + csvSourceFile.getName().substring(lastDotIndex);
            File newCsvFile = new File(destDir+"/filtered/"+outputfile);

            if(!newCsvFile.exists()) {
                FileWriter newCsvFileWriter = new FileWriter(newCsvFile);
             //  CSVWriter writer = new CSVWriter(newCsvFileWriter);
               CSVWriter writer = new CSVWriter(newCsvFileWriter,csvDelimeter,'\0',
                        CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.DEFAULT_LINE_END);
                try (CSVReader reader = new CSVReader(new FileReader(csvSourceFile.getAbsolutePath()))) {
                    List<String[]> r = reader.readAll();
                    writer.writeNext(r.get(0));
                    for (String[] row : r) {
                        for (String cell : row) {
                            if (cell.contains(filterValue)) {
                                writer.writeNext(row);
                            }
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (CsvException e) {
                    e.printStackTrace();
                }
                writer.close();
            } else {
                throw new FileAlreadyExistsException(newCsvFile.getName());
            }
        }
        logger.info("filterCsvFile ends: " + filterValue);
    }

    /**
     *
     * @param keyName
     * @param uploadFilePath
     */
    @Override
    public void uploadFile(String keyName, String uploadFilePath) {
        File uploadDir = new File(uploadFilePath);
        File [] uploadFilesList = uploadDir.listFiles(obj -> obj.isFile() && obj.getName().endsWith(".csv"));

        try {
            for(File uploadFile:uploadFilesList){
                s3client.putObject(new PutObjectRequest(bucketName, keyName, uploadFile));
                logger.info("===================== Upload File "+ uploadFile.getName()+" - Done! =====================");
            }
        } catch (AmazonServiceException ase) {
            logger.info("Caught an AmazonServiceException from PUT requests, rejected reasons:");
            logger.info("Error Message:    " + ase.getMessage());
            logger.info("HTTP Status Code: " + ase.getStatusCode());
            logger.info("AWS Error Code:   " + ase.getErrorCode());
            logger.info("Error Type:       " + ase.getErrorType());
            logger.info("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            logger.info("Caught an AmazonClientException: ");
            logger.info("Error Message: " + ace.getMessage());
        }
    }
}

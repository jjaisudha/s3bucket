package com.nuix.s3fileprocessing.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.config.model.MessageType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;



@Service
public class S3ServiceImpl implements S3Service {

    private Logger logger = LoggerFactory.getLogger(S3ServiceImpl.class);
    @Autowired
    private AmazonS3 s3client;

    @Value("${s3.bucket}")
    private String bucketName;



    @Override
    public void downloadFile(String keyName) {
        ObjectListing inputFileObjects = null;
        String fileKey = null;
        byte[] buffer = new byte[1024];
        File destDir = new File("src/main/resources/");
        // Get summary information for all objects in the input bucket
        inputFileObjects = s3client.listObjects(bucketName);
        do {
            // Iterate over the list of object summaries
            // Get the object key from each object summary
            for (S3ObjectSummary objectSummary : inputFileObjects.getObjectSummaries()) {
                fileKey = objectSummary.getKey();
                System.out.println("DataTransformer: Transforming file: " + fileKey);

                if (fileKey.endsWith(".zip")) {
                    // Retrieve the object with the specified key from the input bucket
                    S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, fileKey));
                    ZipInputStream zis = new ZipInputStream(s3object.getObjectContent());
                    try {
                        ZipEntry entry = zis.getNextEntry();

                        while(entry != null) {
                            String fileName = entry.getName();
                            File newFile = new File(destDir, entry.getName());

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
                                    while (is.available() > 0)
                                    {
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

                    System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());

                }
                inputFileObjects = s3client.listNextBatchOfObjects(inputFileObjects);
            }
        } while (inputFileObjects.isTruncated());
    }

    /*public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
        logger.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
        String rawSchema = getSchema(csvFile);
        if(outputParquetFile.exists()) {
            throw new IOException("Output file " + outputParquetFile.getAbsolutePath() +
                    " already exists");
        }

        Path path = new Path(outputParquetFile.toURI());

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int lineNumber = 0;
        try {
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
                writer.write(Arrays.asList(fields));
                ++lineNumber;
            }

            writer.close();
        } finally {
            logger.info("Number of lines: " + lineNumber);
           // Utils.closeQuietly(br);
        }
    }

    public static String getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }
    private static String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null ) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            //Utils.closeQuietly(reader);
        }

        return stringBuilder.toString();
    }
*/

    @Override
    public void uploadFile(String keyName, String uploadFilePath) {

        try {

            File file = new File(uploadFilePath);
            s3client.putObject(new PutObjectRequest(bucketName, keyName, file));
            logger.info("===================== Upload File - Done! =====================");

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

    public static void displayText(InputStream input) throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            System.out.println("    " + line);
        }
    }

}

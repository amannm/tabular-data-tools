package com.amannmalik.tabulardatatools.gateway;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

public class AwsStorageGateway implements StorageGateway {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    public void get(URI uri, Path path) {
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        File file = path.toFile();
        try (FileOutputStream result = new FileOutputStream(file)) {
            S3Object o = s3.getObject(amazonS3URI.getBucket(), amazonS3URI.getKey());
            S3ObjectInputStream s3is = o.getObjectContent();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = s3is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            s3is.close();
        } catch (AmazonServiceException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(URI uri, Path path) {
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        File file = path.toFile();
        try {
            s3.putObject(new PutObjectRequest(amazonS3URI.getBucket(), amazonS3URI.getKey(), file));
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(URI uri) {
        AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
        try {
            s3.deleteObject(new DeleteObjectRequest(amazonS3URI.getBucket(), amazonS3URI.getKey()));
        } catch (AmazonServiceException e) {
            throw new RuntimeException(e);
        }
    }
}

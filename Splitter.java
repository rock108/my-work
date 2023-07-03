package com.db.acs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Splitter {

    public static void main(String[] args) {
        long startTime = System.nanoTime();

        try {
            RandomAccessFile aFile = new RandomAccessFile("E:\\campaigns.csv", "r");
            FileChannel inChannel = aFile.getChannel();
            long fileSize = aFile.length();
            int chunkSize = 1024*50; // 500MB
            long numChunks = (long) Math.ceil((double) fileSize / chunkSize);
            int count=0;
            byte[] rem=new byte[0];
            byte[] header=null;
            for (long chunk = 0; chunk < numChunks; chunk++) {
                long offset = chunk * chunkSize;
                long size = Math.min(chunkSize, fileSize - offset);
                // Map the file into memory for the current chunk
                MappedByteBuffer buffer = aFile.getChannel().map(FileChannel.MapMode.READ_ONLY, offset, size);
                // Specify the character encoding
                final String encoding = StandardCharsets.UTF_8.name();
                String path="E:\\part_"+chunk+".csv";
               // Convert the MappedByteBuffer to a byte array
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                if(chunk==0){
                    for(int start=0;start<bytes.length;start++){
                        if(bytes[start]==0xA){
                            header=Arrays.copyOfRange(bytes,0,start+1);
                            break;
                        }
                    }
                    Files.write(Paths.get(path),rem,StandardOpenOption.CREATE_NEW);
                }else{
                    Files.write(Paths.get(path),header,StandardOpenOption.CREATE_NEW);
                    Files.write(Paths.get(path),rem,StandardOpenOption.APPEND);
                }
                if(bytes[bytes.length-1]==0xA){
                    Files.write(Paths.get(path),bytes,StandardOpenOption.APPEND);
                    rem=new byte[0];
                    break;
                }else{
                    for(int j=bytes.length-1;j>=0;j--){
                        if(bytes[j]==0xA){
                            byte[] main=Arrays.copyOfRange(bytes,0,j);
                            byte[] pending=Arrays.copyOfRange(bytes,j+1,bytes.length);
                            Files.write(Paths.get(path),main, StandardOpenOption.APPEND);
                            bytes=null;
                            rem=pending;
                            break;
                        }
                    }
                }
            }
            aFile.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms");
    }
}

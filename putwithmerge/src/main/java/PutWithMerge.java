import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Alexey Filyushin on 11/1/14.
 */
public class PutWithMerge {
    public static void main(String[] args){
        Configuration conf = new Configuration();

        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        try{
            FileSystem hdfs = FileSystem.get(new URI(args[1]), conf);
            FileSystem local = FileSystem.get(conf);

            FileStatus[] inputFiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsFile);

            for (int i = 0;  i < inputFiles.length; i++){
                System.out.println(inputFiles[i].getPath().getName());
                FSDataInputStream in = local.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;

                while ((bytesRead = in.read(buffer)) > 0){
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        }catch (IOException e){
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }
}
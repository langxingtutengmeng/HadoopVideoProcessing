package com.lbl.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by 22731 on 2017/11/23.
 */
public class ImageToSeq {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/hadoop/project"));
        conf.addResource(new Path("/hadoop/project"));
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path("mapin/");
        Path outPath = new Path("mapout/");
        FSDataInputStream in = null;
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;
        try {
            in = fs.open(inPath);
            byte[] buffer = new byte[in.available()];
            in.read(buffer);
            writer = SequenceFile.createWriter(fs, conf, outPath, key.getClass(), value.getClass());
            writer.append(new Text(inPath.getName()), new BytesWritable(buffer));
        } catch (Exception e) {
            System.out.println("Exception MESSAGES = " + e.getMessage());
        } finally {
            IOUtils.closeStream(writer);
            System.out.println("last line of the code...!!!!");
        }
    }
}

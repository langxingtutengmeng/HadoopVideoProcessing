package com.lbl.hadoop.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.javacv.OpenCVFrameGrabber;

import java.io.IOException;
import java.nio.ByteBuffer;

public class VideoRecordReader extends RecordReader<Text, BytesWritable> {

    private long start;
    private long pos;
    private long end;
    private String keyName;
    private Text key;
    private BytesWritable value;

    private byte[] buf;

    private OpenCVFrameGrabber grabber;
    private OpenCVFrameConverter.ToMat converter;
    private int currentFrameNumber;
    private int totalFrameNumber;
    private Mat frame;
    private ByteBuffer byteBuffer;

    public VideoRecordReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        start = split.getStart();
        end = start + split.getLength();
        this.pos = start;

        keyName = split.getPath().getName();
        buf = new byte[(int) split.getLength()];

        grabber = new OpenCVFrameGrabber(keyName);
        converter = new OpenCVFrameConverter.ToMat();
        grabber.start();
        totalFrameNumber = grabber.getLengthInFrames();
        currentFrameNumber = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            while ((frame = converter.convert(grabber.grab())) != null) {
                ++currentFrameNumber;
                key.set(keyName);
                byteBuffer = frame.getByteBuffer();
                value.set(new BytesWritable(byteBuffer.array()));
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    @Override

    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return new BytesWritable();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        try {
            grabber.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

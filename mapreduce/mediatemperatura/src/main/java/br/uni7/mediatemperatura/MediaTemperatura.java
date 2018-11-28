package br.uni7.mediatemperatura;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MediaTemperatura {

	public static void main(String[] args) throws IOException {
		JobClient jobMediaTemperatura = new JobClient();
		
		JobConf jobConf = new JobConf(MediaTemperatura.class);
		jobConf.setJobName("MediaTemperaturaPorMinuto");
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);
		
		jobConf.setMapperClass(MediaTemperaturaMapper.class);
		jobConf.setReducerClass(MediaTemperaturaReducer.class);
		
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		jobMediaTemperatura.setConf(jobConf);
		
		try {
			JobClient.runJob(jobConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}

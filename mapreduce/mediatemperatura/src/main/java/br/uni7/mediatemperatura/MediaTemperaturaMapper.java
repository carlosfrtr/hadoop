package br.uni7.mediatemperatura;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MediaTemperaturaMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private static final String HH_MM_SS = "HH:mm:ss";

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
//		OBTER TEMPERATURA
		Double temperatura = Double.valueOf(getValorFormatado(value, 0));
		
//		OBTER TEMPO
		LocalTime localTime = LocalTime.parse(getValorFormatado(value, 1), DateTimeFormatter.ofPattern(HH_MM_SS));
		
		output.collect(new Text(localTime.toString()), new DoubleWritable(temperatura));
	}

	private String getValorFormatado(Text value, int index) {
		return value.toString().split(";")[index].trim().replaceAll("\"", "");
	}

}

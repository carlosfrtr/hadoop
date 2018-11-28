package br.uni7.mediatemperatura;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MediaTemperaturaReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		Double temperatura = 0.0;
		int qtd = 0;
		
		while (values.hasNext()) {
			DoubleWritable writable = values.next();
			temperatura += writable.get(); // SOMA DAS TEMPERATURAS
			qtd++; // QUANTIDADE DE REGISTROS

		}
		DecimalFormat df = new DecimalFormat("#,00");
		Double media = Double.parseDouble(df.format(temperatura / qtd)); // MÉDIA DO RESULTADO PELA QUANTIDADE
		
		output.collect(key, new DoubleWritable(media));
	}

}

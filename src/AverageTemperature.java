import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver (classe main): configura e submete o job MapReduce que calcula a temperatura média de
 * {@code forestfireinput.csv}.
 *
 * <pre>
 * Map:      linha                       -> ("temperatura", (temp, 1))
 * Combiner: soma a saída de cada Map    -> ("temperatura", (soma parcial, quantidade parcial))
 * Shuffle:  agrupa pela chave           -> ("temperatura", [(s1, n1), (s2, n2), ...])
 * Reduce:   soma(somas) / soma(quant.)  -> ("temperatura", média)
 * </pre>
 *
 * <p>Uso: {@code hadoop jar average-temperature-combiner-1.0.0.jar <entrada> <diretório de saída>}. O
 * diretório de saída ainda não pode existir; o Hadoop se recusa a sobrescrevê-lo.
 */
public class AverageTemperature {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: AverageTemperature <entrada> <diretório de saída>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "temperatura media");
        // Indica ao Hadoop qual jar enviar aos nós que executam as tarefas.
        job.setJarByClass(AverageTemperature.class);

        job.setMapperClass(TemperatureMapper.class);
        // Otimização da Aula 06: agrega a saída de cada Map antes de enviá-la ao Reducer.
        job.setCombinerClass(TemperatureCombiner.class);
        job.setReducerClass(TemperatureReducer.class);

        // Os tipos de saída do Map são diferentes dos tipos da saída final, então os dois pares são declarados.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

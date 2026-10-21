import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner: agrega a saída de cada Map antes do sort/shuffle (Aula 06).
 *
 * <p>Sem o Combiner, cada leitura vira um registro enviado ao Reducer: 517 pares
 * {@code ("temperatura", (temp, 1))}. Com ele, cada Map junta os próprios pares em um só,
 * {@code ("temperatura", (soma parcial, quantidade parcial))}, e o Reducer recebe um registro por
 * Map em vez de um por linha. Menos dados trafegam pela rede e o Reducer itera sobre menos valores.
 *
 * <p>Regras que o Combiner precisa seguir:
 * <ul>
 *   <li>Os tipos de entrada e de saída são os tipos de saída do Map ({@link Text} /
 *       {@link SumCountWritable}), porque a saída dele segue para o Reducer como se tivesse saído
 *       do Map.</li>
 *   <li>Ele <b>não divide</b>. Se emitisse a média parcial, o Reducer acabaria tirando a média das
 *       médias, o erro mostrado na Aula 05. Ele só soma, e a divisão continua sendo feita uma
 *       única vez, no Reducer.</li>
 *   <li>O Hadoop pode executá-lo zero, uma ou várias vezes, então o resultado final não pode
 *       depender dele. Somar somas e quantidades garante isso.</li>
 * </ul>
 */
public class TemperatureCombiner extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

    // Reutilizado entre as chamadas: o context.write serializa o valor na hora, então é seguro.
    private final SumCountWritable partial = new SumCountWritable();

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;
        // Soma a quantidade de cada valor em vez de 1, porque o Combiner pode receber somas parciais
        // de uma execução anterior dele mesmo.
        for (SumCountWritable value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        partial.set(sum, count);
        context.write(key, partial);
    }
}

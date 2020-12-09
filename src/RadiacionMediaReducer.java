
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Alex
 */
public class RadiacionMediaReducer
        extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private float media = 0;
    private FloatWritable result = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        int tam = 0;

        for (FloatWritable val : values) {
            media = media + val.get();
            tam++;
        }
        media = media / tam;
        result.set(media);
        context.write(key, result);
    }
}
    
    /* Reducer: Código de las funciones Reduce, que
procesan los datos recibidos por el Mapper y graban su
salida en el HDFS.*/

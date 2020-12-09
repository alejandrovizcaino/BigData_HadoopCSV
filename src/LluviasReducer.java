
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Alex
 */
public class LluviasReducer
        extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

    private FloatWritable result = new FloatWritable();

    @Override
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        float sum = 0;

        for (FloatWritable val : values) {
            sum = sum + val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

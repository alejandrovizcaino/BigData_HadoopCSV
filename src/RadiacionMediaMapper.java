
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author Alex
 */
public class RadiacionMediaMapper
        extends Mapper<LongWritable, Text, Text, FloatWritable> {

    /* Mapper: Código de las funciones Map, que procesan un split de datos de entrada.*/
    private FloatWritable midecimal = new FloatWritable();
    private ArrayList<Integer> ids = new ArrayList(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10));
    private Text mitexto = new Text();
    Log log = (Log) LogFactory.getLog(RadiacionMediaMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            if (key.get() == 0) {
                return;
            } else {

                String strFile = value.toString();
                String[] datos = strFile.split(";");

                datos[2] = datos[2].replace("\"", "");
                datos[16] = datos[16].replace(",", ".");
                Integer id_subestacion = Integer.parseInt(datos[2]);
             
                if (ids.contains(id_subestacion)) {

                    if (datos[16] != null && !datos[16].isEmpty()) {
                        
                        float radiacion = Float.parseFloat(datos[16]);
                        mitexto.set(datos[3]);
                        midecimal.set(radiacion);
                        //log.info("id: " + id_subestacion + " - radiacion: " + radiacion);
                        context.write(mitexto, midecimal);
                    }

                } else {
                    //log.info("no entro y mi id es "+ id_subestacion);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

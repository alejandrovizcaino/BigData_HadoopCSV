

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author alejandro
 */
public class LluviasMapper
        extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

    private IntWritable mientero = new IntWritable();
    private FloatWritable midecimal = new FloatWritable();
    Log log = (Log) LogFactory.getLog(LluviasMapper.class);
    String elmunicipio = "";

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        
        Configuration c  = context.getConfiguration();
        String miruta = c.get("ruta");
        BufferedReader objReader = null;
        String municipio="";
        String valor ="";
        ArrayList<String> municipios = new ArrayList();
        ArrayList<Double> valores = new ArrayList();
        
        try {
            String str;
            objReader = new BufferedReader(new FileReader(miruta));
            
            while ((str = objReader.readLine()) != null) {
          
                int ind1 = str.indexOf("\"");
                int ind2 = str.indexOf("\"", str.indexOf("\"") + 1);      
                municipio = str.substring(ind1+1, ind2);
                valor = str.substring(ind2+1, str.length());
                valor = valor.trim();
                municipios.add(municipio);
                valores.add(Double.parseDouble(valor));
            }
        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {
                if (objReader != null) {
                    objReader.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        Double max = -1.0;
        for (int j=0; j < municipios.size(); j++){
            if (valores.get(j)>max){
                max = valores.get(j);
                municipio = municipios.get(j);
            }
        }
        elmunicipio = municipio;

        try {
            if (key.get() == 0) {
                return;
            } else {

                String strFile = value.toString();
                String[] datos = strFile.split(";");
                Integer anio = Integer.parseInt(datos[4].substring(6, 10));
                datos[3] = datos[3].replace("\"", "");

                if (datos[3].equals(elmunicipio)) {

                    datos[17] = datos[17].replace(",", ".");

                    if (datos[17] != null && !datos[17].isEmpty()) {

                        float lluvias = Float.parseFloat(datos[17]);
                        mientero.set(anio);
                        midecimal.set(lluvias);
                        //log.info(datos[3] + "- anio: " + anio + " - lluvias: " + lluvias);
                        context.write(mientero, midecimal);
                    }
                } else {
                    //log.info("no entro");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

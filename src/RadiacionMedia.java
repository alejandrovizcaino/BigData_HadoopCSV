
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author alejandro
 */
public class RadiacionMedia extends Configured implements Tool {
    
    
    /*Driver: Código que se ejecuta para configurar y lanzar el job.*/
    
    public int run(String[] args) throws Exception {
        
        // Rutas de los ficheros
        Path primeraRutaEntrada = new Path(args[0]);
	Path primeraRutaSalida = new Path(args[1]);
	Path segundaRutaSalida = new Path(args[2]);
        
        JobControl jobControl = new JobControl("jobChain");
            
        // Primer Job
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Radiacion Media");
        
        FileInputFormat.addInputPath(job1, primeraRutaEntrada);
        FileOutputFormat.setOutputPath(job1, primeraRutaSalida);

        job1.setJarByClass(RadiacionMedia.class);
        job1.setMapperClass(RadiacionMediaMapper.class);
        job1.setReducerClass(RadiacionMediaReducer.class);
       
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        
        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        
        jobControl.addJob(controlledJob1);
        
        //Segundo Job
        Configuration conf2 = new Configuration();
        conf2.set("ruta", primeraRutaSalida+"/part-r-00000");
        
        Job job2 = Job.getInstance(conf2, "Lluvias");
        
        FileInputFormat.addInputPath(job2, primeraRutaEntrada);
        FileOutputFormat.setOutputPath(job2, segundaRutaSalida);
  
        job2.setJarByClass(RadiacionMedia.class);
        job2.setMapperClass(LluviasMapper.class);
        job2.setReducerClass(LluviasReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class); 
        job2.setMapOutputValueClass(FloatWritable.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(FloatWritable.class);
        
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        
        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        
        while (!jobControl.allFinished()) {
        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
        try {
            Thread.sleep(5000);
            } catch (Exception e) {

            }
        } 
        System.exit(0);  
        return (job1.waitForCompletion(true) ? 0 : 1);   
   
    }
    
    public static void main(String[] args) throws Exception { 
        int exitCode = ToolRunner.run(new RadiacionMedia(), args);  
        System.exit(exitCode);
    }

    
   
}

package solution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
 * To define a map function for your MapReduce job, subclass
 * the Mapper class and override the map method.
 * The class definition requires four parameters:
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type
 *   for the reducer)
 *   The data type of the output value (which is the input value
 *   type for the reducer)
 */

public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
     * The map method runs once for each line of text in the input file.
     * The method receives a key of type LongWritable, a value of type
     * Text, and a Context object.
     */


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        /*
         * Convert the line, which is received as a Text object,
         * to a String object.
         */
        String line = value.toString();

        /*
         * Hace substring de la línea para seleccionar la posición 0 hasta encontrar un espacio.
         * luego trimea para quitar el espacio en blanco final e incial en caso de haber
         */
        String ip = line.substring(0, line.indexOf(" ")).trim();
        if (ip.length() > 0)
        {
            /*
             * Validamos que lo que haya llegado sea un ip, si no lo es no lo pinta en el archivo
             * Revisando el tiempo de procesamiento no he notado gran diferencia entre
             * usar este metodo o no
             */
            if(validateIPAddress(ip))
                context.write(new Text(ip), new IntWritable(1));
        }


    }

    /*
     * Metodo que valida ips, separa mediante expresión regular el ip en 4 partes y comprueba que
     * cada una de las partes esté entre 0 y 255 dando como resultado true en caso de ip valido
     * o false en caso de no serlo o de recibir un valor distinto ( o fallar)
     */
    public boolean validateIPAddress(String ipAddress) {
        String[] tokens = ipAddress.split("\\.");
        if (tokens.length != 4) {
            return false;
        }
        for (String str : tokens) {
            try {
                int i = Integer.parseInt(str);
                if ((i < 0) || (i > 255)) {
                    return false;
                }
            }catch(NumberFormatException e){
                return false;
            }
        }
        return true;
    }


}
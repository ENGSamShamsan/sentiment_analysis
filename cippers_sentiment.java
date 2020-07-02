package cippers_sentiment;

import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

public class HW2_Part1 {

    private final static String recordRegex = ",";
    private final static Pattern REGEX = Pattern.compile(recordRegex);
    static int msymbol = 0;
    static int mdividend = 4;
    static int mprice = 5;
    static String mNull = "";
    static String mheader;
    
    private static boolean testing = true;

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Arguments provided:  ");
            for (String arg : args) {
                System.out.println(arg);
            }
            System.out.printf("Usage: Provide <input dir> <output dir> \n");
            System.out.printf("Example: data/companies/SP500-constituents-financials.csv output/hw2_1 \n");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.setMaster("local");
        conf.setAppName("HW2 Part 1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String outputPath = args[1] + "_" + Calendar.getInstance().getTimeInMillis();

        final Accumulator<Integer> validAccumulator = sc.accumulator(0);
        final Accumulator<Integer> notValidAccumulator = sc.accumulator(0);
        final Accumulator<Integer> noDividendAccumulator = sc.accumulator(0);
        JavaRDD<String> lines = sc.textFile(args[0]);

        if (testing) {
         
            lines.cache();
            
            System.out.println(lines.first());
        }
        mheader = lines.first();
        JavaRDD<String> dataNoHeader = lines.filter(s -> !s.equals(mheader));

        JavaRDD<Tuple3<String, String, String>> stockInfo = dataNoHeader
                .map(new Function<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> call(String line) throws Exception {
                        String[] attList = line.split(",");
                        String strSymbol, strDividend, strPrice;

                        strSymbol = attList[msymbol];
                        if (attList[mdividend].equals(mNull)) {
                            strDividend = "0.0";
                            noDividendAccumulator.add(1);
                        } else {
                            strDividend = attList[mdividend];
                        }
                        if (attList[mprice].equals(mNull)) {
                            strPrice = String.valueOf(Float.NEGATIVE_INFINITY);
                        } else {
                            strPrice = attList[mprice];
                        }
                        return new Tuple3<String, String, String>(strSymbol, strDividend, strPrice);
                    }
                });

     
        JavaRDD<Tuple3<String, String, String>> filteredInfo = stockInfo
                .filter(new Function<Tuple3<String, String, String>, Boolean>() {
                    @SuppressWarnings("deprecation")
					@Override
                    public Boolean call(Tuple3<String, String, String> v1) throws Exception {
                        try {
                            Float.valueOf(v1._2());
                            Float.valueOf(v1._3());
                            validAccumulator.add(1);
                            return true;
                        } catch (NumberFormatException e) {
                            notValidAccumulator.add(1);
                            return false;
                        }
                    }
                });

        JavaRDD<Tuple3<String, String, String>> sortedInfo = filteredInfo
                .sortBy(new Function<Tuple3<String, String, String>, String>() {

                    @Override
                    public String call(Tuple3<String, String, String> info) throws Exception {
                        return info._1();
                    }

                }, true, 1);

       
        sortedInfo.saveAsTextFile(outputPath);

        System.out.println("No Dividend records:  " + noDividendAccumulator.value());
        System.out.println("Not Valid records:  " + notValidAccumulator.value());
        System.out.println("Valid records:  " + validAccumulator.value());

        sc.close();
    }
}


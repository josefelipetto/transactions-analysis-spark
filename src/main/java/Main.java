import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple10;
import scala.Tuple2;

/**
 * Alunos: José Henrique Medeiros Felipetto
 *         Tiago Paiva
 * The type Main.
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("transactionsAnalysis").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // At this point, we're going to create a initial RDD with the basic file structure provided
        // in the exercise description.
        JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> initialRdd =
                sparkContext
                    .textFile("files/transactions.csv")
                    .map(line -> {
                        String[] parts = line.split(";");
                        return new Tuple10<>(
                                parts[TransactionsConstants.COUNTRY],
                                parts[TransactionsConstants.YEAR],
                                parts[TransactionsConstants.CODE],
                                parts[TransactionsConstants.GOOD],
                                parts[TransactionsConstants.FLOW],
                                parts[TransactionsConstants.PRICE],
                                parts[TransactionsConstants.WEIGHT],
                                parts[TransactionsConstants.UNITY],
                                parts[TransactionsConstants.QUANTITY],
                                parts[TransactionsConstants.CATEGORY]
                        );
                    });

        // Exercise 1
        numberOfTransactionsByGoodInBrazil(initialRdd);

        // Exercise 2
        numberOfTransactionsByYear(initialRdd);

        // Exercise 3
        mostImportedGoodIn2016InBrazil(initialRdd);

        // Exercise 4
        weightAverageOfGoodPerYear(initialRdd);

        // Exercise 5 -> For this one we just need to pass a RDD already filtered with transactions made in Brazil
        // to the function used in exercise 4
        weightAverageOfGoodPerYear(initialRdd.filter(transaction -> transaction._1().equals("Brazil")));

        /*
          Results:
            ((2010,Cuff-links and studs of base metal, plated or not),9469.5)
            ((1993,Table, kitchenware of low expansion glass (Pyrex etc)),8065914.0)
            ((1995,Plain weave cotton <85% +manmade fibre >200g bleached),84034.0)
            ((1991,Woven fabric <85% artif staple + manmade fil, printed),1759.0)
        */

        // Exercise 6
        productWithBiggestPriceByWeightUnity(initialRdd);

        // Exercise 7
        transactionsByFlowAndYear(initialRdd);

    }

    /**
     * Number of transactions by good made in brazil.
     *
     * Results from println:
     *   (Fluorides of ammonium or of sodium,37)
     *   (Agarbatti, odorifers operated by burning,55)
     *   (Glucose, glucose syrup < 20% fructose,56)
     *   (Trucks nes,40)
     *   (Articles, iron or steel nes, forged/stamped, nfw,70)
     *
     * @param rdd rdd with the transactions
     */
    private static void numberOfTransactionsByGoodInBrazil(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Filter by transactions made in brazil
            .filter(transaction -> transaction._1().equals("Brazil"))
            // Map to a pair of Good -> 1L
            .mapToPair(transaction -> new Tuple2<>(transaction._4(), 1L))
            // Reduce by adding up the lines
            .reduceByKey(Long::sum)
            // Take just the first 5
            .take(5)
            // Prints out the result
            .forEach(System.out::println);
    }

    /**
     * Get the number of transactions per year
     * Results:
     *   (2010,373791)
     *   (2011,374596)
     *   (2012,377343)
     *   (2013,370936)
     *   (2014,364454)
     * @param rdd the rdd
     */
    private static void numberOfTransactionsByYear(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Map to a pair of Year -> 1L
            .mapToPair(transaction -> new Tuple2<>(transaction._2(), 1L))
            // Reduce by adding up lines
            .reduceByKey(Long::sum)
            // Take the first 5
            .take(5)
            // Prints out the result
            .forEach(System.out::println);
    }

    /**
     * Get the most imported good in 2016 in Brazil
     * Results:
     *   (Bituminous coal, not agglomerated,18334200757)
     * @param rdd the rdd
     */
    private static void mostImportedGoodIn2016InBrazil(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Filters transactions made in Brazil, flow is Import and was made in 2016.
            .filter(transaction -> transaction._1().equals("Brazil") && transaction._5().equals("Import") && transaction._2().equals("2016"))
            // Map to a pair of GOOD -> QUANTITY
            .mapToPair(transaction -> new Tuple2<>(transaction._4(), Utils.parseLong(transaction._9())))
            // Swaps the pair to use sortByKey, becoming QUANTITY -> GOOD
            .mapToPair(Tuple2::swap)
            // Sort by key in descending mode
            .sortByKey(false)
            // Take first element
            .take(1)
            // Prints it our
            .forEach(System.out::println);
    }

    /**
     * Get the weight average of goods separated by year
     * Results:
     *   ((1991,Pocket-watch, precious-metal case, non-battery),0.0)
     *   ((2010,Cuff-links and studs of base metal, plated or not),62221.52205882353)
     *   ((1995,Plain weave cotton <85% +manmade fibre >200g bleached),39926.05)
     *   ((1991,Woven fabric <85% artif staple + manmade fil, printed),20591.666666666668)
     *   ((2001,Carpets of manmade yarn, woven pile, made up,nes),1987236.35)
     * @param rdd the rdd
     */
    private static void weightAverageOfGoodPerYear(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Let's aggregate using a Tuple in form of ((YEAR, GOOD) -> WEIGHT)
            .mapToPair(transaction -> new Tuple2<>(new Tuple2<>(transaction._2(), transaction._4()), Utils.parseLong(transaction._7())))
            // Now we aggregate by key. First param we pass the zeroValue, second the function to sum the weight and sum the number of rows, third we sum up the aggregators
            .aggregateByKey(
                    new Tuple2<>(0L,0L),
                    (acc, value) -> new Tuple2<>(acc._1() + value, acc._2() + 1),
                    (acc1, acc2) -> new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2()))
            // Now we have the sum of the weights and the sum of numbers of rows. Let's divide that
            .mapValues(sumCount -> 1.0 * sumCount._1() / sumCount._2())
            // Take just 5
            .take(5)
            // Prints it out
            .forEach(System.out::println);
    }

    /**
     * Get the product with biggest price by weight unity
     * Results:
     *   (Floating, submersible drilling or production platform,474093799)
     * @param rdd the rdd
     */
    private static void productWithBiggestPriceByWeightUnity(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Now we map over the transaction to return a tuple in the form of (PRICEBYWEIGHT, GOOD)
            .mapToPair(transaction -> {

                long price = Utils.parseLong(transaction._6());
                long weight = Utils.parseLong(transaction._7());
                long quantity = Utils.parseLong(transaction._9());

                if (quantity != 0) {
                    price /= quantity;
                    weight /= quantity;
                }

                long priceByWeight = 0L;

                if (weight != 0) {
                    priceByWeight = price / weight;
                }

                return new Tuple2<>(priceByWeight,transaction._4());
            })
            // Sort by key in descending mode
            .sortByKey(false)
            // Swap tuple to become the form of (GOOD, PRICEBYWEIGHT)
            .mapToPair(Tuple2::swap)
            // Take first
            .take(1)
            // Prints it out
            .forEach(System.out::println);
    }

    /**
     * Get the number of transactions by flow and year
     * Results:
     *   ((Re-Export,1995),9221)
     *   ((Import,2002),208980)
     *   ((Import,2001),209374)
     *   ((Re-Export,2000),14530)
     *   ((Import,2016),160235)
     * @param rdd the rdd
     */
    private static void transactionsByFlowAndYear(JavaRDD<Tuple10<String, String, String, String, String, String, String, String, String, String>> rdd)
    {
        rdd
            // Map to a tuple with key being the Flow and Year ((FLOW,YEAR) -> COUNT)
            .mapToPair(transaction -> new Tuple2<>(new Tuple2<>(transaction._5(), transaction._2()), 1L))
            // Reduce it by adding up values
            .reduceByKey(Long::sum)
            // Take 5
            .take(5)
            // Prints it out
            .forEach(System.out::println);
    }
}

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Dmitriy on 07.04.2017.
 */
public class Main {
    private static Duration slideInterval = new Duration(20000);
    private static Duration openWindowInterval = new Duration(120000);

    public static void main(String[] args) throws InterruptedException {
        //Configure hadoop home directory
        System.setProperty("hadoop.home.dir", "D:\\workspace");

        //Creating spark context
        SparkConf conf = new SparkConf().setAppName("appname")
                                        .setMaster("local[*]");

        //Creating streaming context
        JavaStreamingContext scc = new JavaStreamingContext(conf, slideInterval);

        //Configure twitter credentials
        System.setProperty("twitter4j.oauth.consumerKey","KxLiJibMrHEVY1xlBp6YTknId");
        System.setProperty("twitter4j.oauth.consumerSecret","TlvQsYWm2u8iuBo4DBQHiO5OYKTbq0ksznPCjNyiNh6i0lpqma");
        System.setProperty("twitter4j.oauth.accessToken","850254350987350017-QwhynLnYNqAybWP6eB3K2BQCRBdoyfv");
        System.setProperty("twitter4j.oauth.accessTokenSecret","AZVsNyGtVVSCyryqOsPRoOZgrqKTfQViO3XCpBdpGJMau");

        //Configure filtering words
        String[] filters={"#news", "#Playingnow"};

        //Creating tweets stream
        JavaDStream<Status> tweets = TwitterUtils.createStream(scc, null, filters)
                                                 .window(openWindowInterval, slideInterval);

        //Getting word from tweets
        JavaDStream<String> words = tweets.map(tweet -> tweet.getText())
                                          .flatMap(text -> split(text))
                                          .filter(string -> !string.contains("https") &&
                                                            !string.contains("RT") &&
                                                            !string.contains("t") &&
                                                            !string.contains("co"));

        //Saving each word to spark sql database
        words.foreachRDD((rdd, time) -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

            //Mapping each rdd to word entity
            JavaRDD<Word> rowRDD = rdd.map(word -> {
                Word record = new Word();
                record.setWord(word);
                return record;
            });

            //Adding words to spark sql database
            DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, Word.class);
            wordsDataFrame.registerTempTable("words");

            //Query to show most popular words in spark sql database
            DataFrame wordCountsDataFrame = sqlContext.sql(
                    "select word, count(*) as total " +
                            "from words group by word order by total desc limit 10");

            wordCountsDataFrame.show();
            return null;
        });

        scc.start();
        scc.awaitTermination();
    }

    public static List<String> split(String string){
        String [] words = string.toLowerCase()
                                .replaceAll("[^A-Za-z0-9 ]", " ")
                                .split(" ");

        List<String> filteredWords = Arrays.asList(words);
        filteredWords.removeIf(word -> word.isEmpty() || !word.matches("[a-zA-Z]+"));
        return filteredWords;
    }
}

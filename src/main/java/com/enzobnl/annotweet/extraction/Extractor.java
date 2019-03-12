package com.enzobnl.annotweet.extraction;

import com.enzobnl.annotweet.extraction.StatusFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Extraction of tweets dataset example:
 *
 * Extractor extractor = new Extractor(...API credentials...);
 * Files.newBufferedWriter(path).write(extractor.savingReadyString());
 */
public class Extractor {
    enum ExtractionErrorMode{
        THROW,
        SIMULATE // Used if you want to test the class but twitter API is not reachable for any reason
    }

    String consumerKey;
    String consumerSecret;
    String accessToken;
    String accessTokenSecret;

    public Extractor(String consumerKey,  String consumerSecret, String accessToken, String accessTokenSecret){
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }

    /**
     * Extract twitter4j.Status from API (or emulate them if API not reachable and 'eem' set to SIMULATE). Then converts
     * them into String by mapping provided formatTweet function on status List and then returns the resulting
     * Stream<String>
     *
     * @param formatTweet
     * @param eem
     * @return Stream<String> of extracted and formatted tweets status
     * @throws TwitterException
     */
    public Stream<String> extract(BiFunction<Long, String, String> formatTweet, ExtractionErrorMode eem) throws TwitterException {
        Stream<Status> statusStream = null;
        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessTokenSecret);
            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();
            Query query = new Query("macron");
            QueryResult result = twitter.search(query);
            statusStream = result.getTweets().stream();
        } catch (TwitterException e){
            switch(eem){
                case SIMULATE:
                    List<Status> statusList = new ArrayList<>();
                    for(long i=0; i<15; i++)statusList.add(StatusFactory.create(i, String.format("tweet text %d", i)));
                    statusStream = statusList.stream();
                    break;
                case THROW:
                    throw e;
            }
        }
        return statusStream.map((Status s) -> formatTweet.apply(s.getId(), s.getText()));
    }

    /**
     * Provided a way to build our dataset with the desired format for our project: "(id, tag) tweet_text"
     * @return String ready to be saved in file dataset.txt
     * @throws TwitterException
     */
    public String savingReadyString() throws TwitterException {
        BiFunction<Long, String, String> formatTweet = (Long id, String text) -> String.format("(%d,???) %s", id, text.replace("\r", "").replace("\n", ";"));
        return extract(formatTweet, ExtractionErrorMode.SIMULATE).reduce((String lines, String line) -> String.format("%s\n%s", lines, line)).get();
    }
}

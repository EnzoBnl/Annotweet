package com.enzobnl.annotweet;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class Main {
    public static void main(String[] args) throws TwitterException {
        /*
            Consumer API keys
            ZThRYrFA1yvemxQDg9RmJ6xRd (API key)

            cTohJXmrXacK3vY8PcbyB1R8hY0Yf65rdqsueooBvmJ9hki73p (API secret key)
            Access token & access token secret
            1100680212700909568-eob1SMXnnU9Rz0DJYzjeF4tR43Jv7Z (Access token)

            110INrsKORQRP13IkFxmfvowUFY0TgwdQ3C6H5BzlS6R3 (Access token secret)
         */
        System.out.println("bla");
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("ZThRYrFA1yvemxQDg9RmJ6xRd")
                .setOAuthConsumerSecret("cTohJXmrXacK3vY8PcbyB1R8hY0Yf65rdqsueooBvmJ9hki73p")
                .setOAuthAccessToken("1100680212700909568-eob1SMXnnU9Rz0DJYzjeF4tR43Jv7Z")
                .setOAuthAccessTokenSecret("110INrsKORQRP13IkFxmfvowUFY0TgwdQ3C6H5BzlS6R3");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        // The factory instance is re-useable and thread safe.
        Query query = new Query("source:twitter4j ");
        QueryResult result = twitter.search(query);
        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }
    }
}

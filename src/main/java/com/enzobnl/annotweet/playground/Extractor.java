package com.enzobnl.annotweet.playground;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class Extractor {
    public static void main(String[] args) throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("ZThRYrFA1yvemxQDg9RmJ6xRd")
                .setOAuthConsumerSecret("cTohJXmrXacK3vY8PcbyB1R8hY0Yf65rdqsueooBvmJ9hki73p")
                .setOAuthAccessToken("1100680212700909568-eob1SMXnnU9Rz0DJYzjeF4tR43Jv7Z")
                .setOAuthAccessTokenSecret("110INrsKORQRP13IkFxmfvowUFY0TgwdQ3C6H5BzlS6R3");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        Query query = new Query("macron");
        QueryResult result = twitter.search(query);
        for (Status status : result.getTweets()) {
            System.out.println("(" + status.getId() + "," + "???" + ") "
                    + status.getText().replace("\r", "").replace("\n", ";"));
        }
    }
}

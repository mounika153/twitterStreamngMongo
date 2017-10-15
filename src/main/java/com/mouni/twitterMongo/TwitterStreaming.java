package com.mouni.twitterMongo;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.mongodb.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


@SpringBootApplication
public class TwitterStreaming {
    public static void main(String[] args) {
        try
        {
            Mongo m = new Mongo("localhost", 27017);
            DB db = m.getDB("twitterdb");
//            int tweetCount = 100;

            final DBCollection coll = db.getCollection("tweets");

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey("f5lIaz3KL9waImfXT5FxshumB")
                    .setOAuthConsumerSecret("WFn3z5Kz3O2WDYIiLKc42S5ASu7EvkwwsnSWpodH1H8vI1jNFV")
                    .setOAuthAccessToken("782647345-Z6Ao7wTcK8adVFOZ3u6KDQobMabiNImuET24DuNs")
                    .setOAuthAccessTokenSecret("0Iul8w6NxEYuNBdkKoETPqW3wNHCE0lXqNrL6GQtELeik");

            StatusListener listener = new StatusListener(){
                int count = 0;
                public void onStatus(Status status) {
                    System.out.println(status.getId() +  " : " + status.getSource()+ " : " +status.getCreatedAt()+ " : " + status.getUser().getName() + " : " +status.getText());
                    System.out.println(status.getUser().getName() + " : " + status.getText());

                    DBObject dbObj = new BasicDBObject();
                    dbObj.put("id_str", status.getId());
                    dbObj.put("name", status.getUser().getName());
                    dbObj.put("text", status.getText());
                    dbObj.put("source", status.getSource());
                    if(status.getGeoLocation() != null) {
                        DBObject pos = new BasicDBObject();
                        pos.put("long", status.getGeoLocation().getLongitude());
                        pos.put("lat", status.getGeoLocation().getLatitude());
                        dbObj.put("pos", pos);
                    } else if( status.getPlace() != null ) {
                        dbObj.put("country", status.getPlace().getCountry());
                    }
                    coll.insert(dbObj);
                    System.out.println(++count);
                }
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
                @Override
                public void onScrubGeo(long arg0, long arg1) {
                    // TODO Auto-generated method stub

                }
                @Override
                public void onStallWarning(StallWarning arg0) {
                    // TODO Auto-generated method stub

                }
            };

            TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
            twitterStream.addListener(listener);
            // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
            twitterStream.sample();

        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}

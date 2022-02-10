package io.hstream.example;

import io.hstream.*;

/** This example shows how to create a subscription associated to the specified stream */
public class CreateSubscriptionExample {
  private static final String SERVICE_URL = "localhost:6570";
  private static final String DEMO_STREAM = "demo_stream";
  private static final String DEMO_SUBSCRIPTION = "demo_subscription";

  public static void main(String[] args) {
    HStreamClient client = HStreamClient.builder().serviceUrl(SERVICE_URL).build();
    // create a subscription which consume data from the tail of the stream.
    Subscription subscriptionFromEarlist =
        Subscription.newBuilder().subscription(DEMO_SUBSCRIPTION).stream(DEMO_STREAM)
            .ackTimeoutSeconds(600)
            .build();
    client.createSubscription(subscriptionFromEarlist);

    // create a subscription which consume data from the tail of the stream.
    Subscription subscriptionFromLatest =
        Subscription.newBuilder().subscription(DEMO_SUBSCRIPTION).stream(DEMO_STREAM)
            .ackTimeoutSeconds(600)
            .build();
    client.createSubscription(subscriptionFromLatest);

    // create a subscription which consume data from specified RecordId.
    Subscription subscription =
        Subscription.newBuilder().subscription(DEMO_SUBSCRIPTION).stream(DEMO_STREAM)
            .ackTimeoutSeconds(600)
            .build();
    client.createSubscription(subscription);
  }
}

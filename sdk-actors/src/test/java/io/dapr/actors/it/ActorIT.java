/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.actors.it;

import io.dapr.DaprGrpc;
import io.dapr.DaprProtos;
import io.dapr.it.*;
import io.dapr.it.DaprIntegrationTestingRunner;
//import io.dapr.it.services.HelloWorldGrpcStateService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.dapr.actors.ActorId;
import io.dapr.actors.client.ActorProxy;
import io.dapr.actors.client.ActorProxyBuilder;
import io.dapr.client.DefaultObjectSerializer;
import io.dapr.actors.services.DemoActorService;



import static io.dapr.it.DaprIntegrationTestingRunner.DAPR_FREEPORTS;

public class ActorIT extends BaseIT {

  private static DaprIntegrationTestingRunner daprIntegrationTestingRunner;

  @BeforeClass
  public static void init() throws Exception {
    System.out.println("**********Enter init");
    daprIntegrationTestingRunner = io.dapr.it.BaseIT.createDaprIntegrationTestingRunner(
      "BUILD SUCCESS",
      io.dapr.actors.services.DemoActorService.class,
      false,
      2000
    );

    System.out.println("********** init, calling initializeDapr");
    daprIntegrationTestingRunner.initializeDapr();
    System.out.println("********** init, returned from initializeDapr");
  }

 @Test
  public void test1() {
    System.out.println("********* *Enter test1");
    // watch the serializer
    ActorProxyBuilder builder = new ActorProxyBuilder("DemoActor", new DefaultObjectSerializer());
    ActorProxy actor = builder.build(new ActorId("maru"));
    actor.invokeActorMethod("registerReminder").block();

    System.out.println("Sleeping");

    try {
      Thread.sleep((long) (100000));
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      return;
    }


  }

}

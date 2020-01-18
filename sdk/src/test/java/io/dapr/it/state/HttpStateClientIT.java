/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.it.state;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprClientBuilder;
import io.dapr.client.domain.StateKeyValue;
import io.dapr.client.domain.StateOptions;
import io.dapr.it.BaseIntegrationTest;
import io.dapr.it.services.EmptyService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Mono;

/**
 * Test State HTTP DAPR capabilities using a DAPR instance with an empty service running
 */
public class HttpStateClientIT extends BaseIntegrationTest {

  @BeforeClass
  public static void init() throws Exception {
    daprIntegrationTestingRunner =
      createDaprIntegrationTestingRunner(
        "BUILD SUCCESS",
        EmptyService.class,
        false,
        0
      );
    daprIntegrationTestingRunner.initializeDapr();
  }

  @Test
  public void saveAndGetState() {

    //The key use to store the state
    final String stateKey = "myKey";

    //create the http client
    DaprClient daprClient = new DaprClientBuilder().build();

    //creation of a dummy data
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //create of the deferred call to DAPR to store the state
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save action
    saveResponse.block();

    //create of the deferred call to DAPR to get the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue(null, stateKey, null, null), null, MyData.class);

    //retrieve the state
    StateKeyValue<MyData> myDataResponse = response.block();

    //Assert that the response is the correct one
    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());
  }

  @Test
  public void saveUpdateAndGetState() {

    //The key use to store the state and be updated
    final String stateKey = "keyToBeUpdated";

    //create http DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();
    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute save action to DAPR
    saveResponse.block();

    //change data properties
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B2");
    //create deferred action to update the sate without any etag or options
    saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the update action to DAPR
    saveResponse.block();

    //Create deferred action to retrieve the action
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the retrieve of the state
    StateKeyValue<MyData> myDataResponse = response.block();

    //review that the update was success action
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B2", myDataResponse.getValue().getPropertyB());
  }

  @Test
  public void saveAndDeleteState() {
    //The key use to store the state and be deleted
    final String stateKey = "myeKeyToBeDeleted";

    //create DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();

    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");
    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save state action
    saveResponse.block();

    //Create deferred action to retrieve the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the retrieve of the state
    StateKeyValue<MyData> myDataResponse = response.block();

    //review that the state was saved correctly
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    //create deferred action to delete the state
    Mono<Void> deleteResponse = daprClient.deleteState(new StateKeyValue<MyData>(null, stateKey, null, null), null);
    //execute the delete action
    deleteResponse.block();

    //Create deferred action to retrieve the state
    response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the retrieve of the state
    myDataResponse = response.block();

    //review that the action does not return any value, because the state was deleted
    Assert.assertNull(myDataResponse.getValue());
  }


  @Test
  public void saveUpdateAndGetStateWithEtag() {
    //The key use to store the state and be updated using etags
    final String stateKey = "keyToBeUpdatedWithEtag";
    //create DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();
    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save state action
    saveResponse.block();

    //Create deferred action to retrieve the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the action for retrieve the state and the etag
    StateKeyValue<MyData> myDataResponse = response.block();

    //review that the etag is not empty
    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    String firstETag = myDataResponse.getEtag();

    //change the data in order to update the state
    data.setPropertyA("data in property A2");
    data.setPropertyB("data in property B2");
    //Create deferred action to update the data using the correct etag
    saveResponse = daprClient.saveState(stateKey, myDataResponse.getEtag(), data, null);
    saveResponse.block();


    response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //retrive the data wihout any etag
    myDataResponse = response.block();

    //review that state value changes
    Assert.assertNotNull(myDataResponse.getEtag());
    //review that the etag changes after an update
    Assert.assertNotEquals(firstETag,myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A2", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B2", myDataResponse.getValue().getPropertyB());
  }


  @Test(expected = RuntimeException.class)
  public void saveUpdateAndGetStateWithWrongEtag() {
    final String stateKey = "keyToBeUpdatedWithWrongEtag";

    //create DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();
    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save state action
    saveResponse.block();

    //Create deferred action to retrieve the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the action for retrieve the state and the etag
    StateKeyValue<MyData> myDataResponse = response.block();

    //review that the etag is not empty
    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    String firstETag = myDataResponse.getEtag();

    //change the data in order to update the state
    data.setPropertyA("data in property A2");
    data.setPropertyB("data in property B2");
    //Create deferred action to update the data using the incorrect etag
    saveResponse = daprClient.saveState(stateKey, "99999999999999", data, null);
    saveResponse.block();


    response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //retrive the data wihout any etag
    myDataResponse = response.block();

    //review that state value changes
    Assert.assertNotNull(myDataResponse.getEtag());
    //review that the etag changes after an update
    Assert.assertNotEquals(firstETag,myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A2", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B2", myDataResponse.getValue().getPropertyB());
  }

  @Test
  public void saveAndDeleteStateWithEtag() {
    final String stateKey = "myeKeyToBeDeletedWithEtag";
    //create DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();
    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");
    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save state action
    saveResponse.block();

    //Create deferred action to get the state with the etag
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the get state
    StateKeyValue<MyData> myDataResponse = response.block();

    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    //Create deferred action to delete an state sending the etag
    Mono<Void> deleteResponse = daprClient.deleteState(new StateKeyValue<MyData>(null, stateKey, myDataResponse.getEtag(), null), null);
    //execute the delete of the state
    deleteResponse.block();

    //Create deferred action to get the sate without an etag
    response = daprClient.getState(new StateKeyValue(null, stateKey, null, null), null, MyData.class);
    myDataResponse = response.block();

    //Review that the response is null, because the state was deleted
    Assert.assertNull(myDataResponse.getValue());
  }


  @Test(expected = RuntimeException.class)
  public void saveAndDeleteStateWithWrongEtag() {
    final String stateKey = "myeKeyToBeDeletedWithWrongEtag";

    //create DAPR client
    DaprClient daprClient = new DaprClientBuilder().build();
    //Create dummy data to be store
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");
    //Create deferred action to save the sate
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, null);
    //execute the save state action
    saveResponse.block();

    //Create deferred action to get the state with the etag
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue<MyData>(null, stateKey, null, null), null, MyData.class);
    //execute the get state
    StateKeyValue<MyData> myDataResponse = response.block();

    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    //Create deferred action to delete an state sending the incorrect etag
    Mono<Void> deleteResponse = daprClient.deleteState(new StateKeyValue<MyData>(null, stateKey, "99999999999", null), null);
    //execute the delete of the state, this should trhow an exception
    deleteResponse.block();

    //Create deferred action to get the sate without an etag
    response = daprClient.getState(new StateKeyValue(null, stateKey, null, null), null, MyData.class);
    myDataResponse = response.block();

    //Review that the response is null, because the state was deleted
    Assert.assertNull(myDataResponse.getValue());
  }

  @Test(expected = RuntimeException.class)
  public void saveUpdateAndGetStateWithEtagAndStateOptionsFirstWrite() {
    final String stateKey = "keyToBeUpdatedWithEtagAndOptions";

    //create option with concurrency with first writte and consistency of strong
    StateOptions stateOptions = new StateOptions(StateOptions.Consistency.STRONG, StateOptions.Concurrency.FIRST_WRITE, null);

    //create dapr client
    DaprClient daprClient = new DaprClientBuilder().build();
    //create Dummy data
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //create state using stateOptions
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, stateOptions);
    //execute the save state
    saveResponse.block();


    //crate deferred action to retrieve the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue(null, stateKey, null, stateOptions), stateOptions, MyData.class);
    //execute the retrieve of the state using options
    StateKeyValue<MyData> myDataResponse = response.block();

    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    //change data to be udpated
    data.setPropertyA("data in property A2");
    data.setPropertyB("data in property B2");
    //create deferred action to update the action with options
    saveResponse = daprClient.saveState(stateKey, myDataResponse.getEtag(), data, stateOptions);
    //update the state
    saveResponse.block();


    data.setPropertyA("last write");
    data.setPropertyB("data in property B2");
    //create deferred action to update the action with the same etag
    saveResponse = daprClient.saveState(stateKey, myDataResponse.getEtag(), data, stateOptions);
    //throws an exception, the state was already udpated
    saveResponse.block();

    response = daprClient.getState(new StateKeyValue(null, stateKey, null, null), stateOptions, MyData.class);
    StateKeyValue<MyData> myLastDataResponse = response.block();

    Assert.assertNotNull(myLastDataResponse.getEtag());
    Assert.assertNotNull(myLastDataResponse.getKey());
    Assert.assertNotNull(myLastDataResponse.getValue());
    Assert.assertNotNull(myDataResponse.getEtag(), myLastDataResponse.getEtag());
    Assert.assertEquals("data in property A2", myLastDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B2", myLastDataResponse.getValue().getPropertyB());
  }

  @Test()
  public void saveUpdateAndGetStateWithEtagAndStateOptionsLastWrite() {
    final String stateKey = "keyToBeUpdatedWithEtagAndOptions";

    //create option with concurrency with first writte and consistency of strong
    StateOptions stateOptions = new StateOptions(StateOptions.Consistency.STRONG, StateOptions.Concurrency.LAST_WRITE, null);

    //create dapr client
    DaprClient daprClient = new DaprClientBuilder().build();
    //create Dummy data
    MyData data = new MyData();
    data.setPropertyA("data in property A");
    data.setPropertyB("data in property B");

    //create state using stateOptions
    Mono<Void> saveResponse = daprClient.saveState(stateKey, null, data, stateOptions);
    //execute the save state
    saveResponse.block();


    //crate deferred action to retrieve the state
    Mono<StateKeyValue<MyData>> response = daprClient.getState(new StateKeyValue(null, stateKey, null, stateOptions), stateOptions, MyData.class);
    //execute the retrieve of the state using options
    StateKeyValue<MyData> myDataResponse = response.block();

    Assert.assertNotNull(myDataResponse.getEtag());
    Assert.assertNotNull(myDataResponse.getKey());
    Assert.assertNotNull(myDataResponse.getValue());
    Assert.assertEquals("data in property A", myDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B", myDataResponse.getValue().getPropertyB());

    //change data to be udpated
    data.setPropertyA("data in property A2");
    data.setPropertyB("data in property B2");
    //create deferred action to update the action with options
    saveResponse = daprClient.saveState(stateKey, myDataResponse.getEtag(), data, stateOptions);
    //update the state
    saveResponse.block();


    data.setPropertyA("last write");
    data.setPropertyB("data in property B2");
    //create deferred action to update the action with the same etag
    saveResponse = daprClient.saveState(stateKey, myDataResponse.getEtag(), data, stateOptions);
    //update the state without an error
    saveResponse.block();

    response = daprClient.getState(new StateKeyValue(null, stateKey, null, null), stateOptions, MyData.class);
    StateKeyValue<MyData> myLastDataResponse = response.block();

    Assert.assertNotNull(myLastDataResponse.getEtag());
    Assert.assertNotNull(myLastDataResponse.getKey());
    Assert.assertNotNull(myLastDataResponse.getValue());
    Assert.assertNotNull(myDataResponse.getEtag(), myLastDataResponse.getEtag());
    Assert.assertEquals("last write", myLastDataResponse.getValue().getPropertyA());
    Assert.assertEquals("data in property B2", myLastDataResponse.getValue().getPropertyB());
  }

}

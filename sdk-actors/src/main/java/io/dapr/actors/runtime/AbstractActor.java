/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.actors.runtime;

import io.dapr.actors.ActorId;
import io.dapr.actors.ActorTrace;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Represents the base class for actors.
 * The base type for actors, that provides the common functionality for actors.
 * The state is preserved across actor garbage collections and fail-overs.
 */
public abstract class AbstractActor {

  private static final ObjectSerializer INTERNAL_SERIALIZER = new ObjectSerializer();

  /**
   * Type of tracing messages.
   */
  private static final String TRACE_TYPE = "Actor";

  /**
   * Context for the Actor runtime.
   */
  private final ActorRuntimeContext<?> actorRuntimeContext;

  /**
   * Actor identifier.
   */
  private final ActorId id;

  /**
   * Emits trace messages for Actors.
   */
  private final ActorTrace actorTrace;

  /**
   * Registered timers for this Actor.
   */
  private final Map<String, ActorTimer> timers;

  /**
   * Manager for the states in Actors.
   */
  private final ActorStateManager actorStateManager;

  /**
   * Internal control to assert method invocation on start and finish in this SDK.
   */
  private boolean started;

  /**
   * Instantiates a new Actor.
   *
   * @param runtimeContext Context for the runtime.
   * @param id             Actor identifier.
   */
  protected AbstractActor(ActorRuntimeContext runtimeContext, ActorId id) {
    this.actorRuntimeContext = runtimeContext;
    this.id = id;
    this.actorStateManager = new ActorStateManager(
          runtimeContext.getStateProvider(),
          runtimeContext.getActorTypeInformation().getName(),
          id);
    this.actorTrace = runtimeContext.getActorTrace();
    this.timers = Collections.synchronizedMap(new HashMap<>());
    this.started = false;
  }

  /**
   * Returns the id of the actor.
   *
   * @return Actor id.
   */
  protected ActorId getId() {
    return this.id;
  }

  /**
   * Returns the state store manager for this Actor.
   *
   * @return State store manager for this Actor
   */
  protected ActorStateManager getActorStateManager() {
    return this.actorStateManager;
  }

  /**
   * Registers a reminder for this Actor.
   *
   * @param reminderName Name of the reminder.
   * @param state        State to be send along with reminder triggers.
   * @param dueTime      Due time for the first trigger.
   * @param period       Frequency for the triggers.
   * @param <T>          Type of the state object.
   * @return Asynchronous void response.
   */
  protected <T> Mono<Void> registerReminder(
        String reminderName,
        T state,
        Duration dueTime,
        Duration period) {
    try {
      byte[] data = this.actorRuntimeContext.getObjectSerializer().serialize(state);
      ActorReminderParams params = new ActorReminderParams(data, dueTime, period);
      byte[] serialized = INTERNAL_SERIALIZER.serialize(params);
      return this.actorRuntimeContext.getDaprClient().registerActorReminder(
            this.actorRuntimeContext.getActorTypeInformation().getName(),
            this.id.toString(),
            reminderName,
            serialized);
    } catch (IOException e) {
      return Mono.error(e);
    }
  }

  /**
   * Registers a Timer for the actor. A timer name is autogenerated by the runtime to keep track of it.
   *
   * @param timerName Name of the timer, unique per Actor (auto-generated if null).
   * @param callback  Name of the method to be called.
   * @param state     State to be passed it to the method when timer triggers.
   * @param dueTime   The amount of time to delay before the async callback is first invoked.
   *                  Specify negative one (-1) milliseconds to prevent the timer from starting.
   *                  Specify zero (0) to start the timer immediately.
   * @param period    The time interval between invocations of the async callback.
   *                  Specify negative one (-1) milliseconds to disable periodic signaling.
   * @param <T>       Type for the state to be passed in to timer.
   * @return Asynchronous result.
   */
  protected <T> Mono<Void> registerActorTimer(
        String timerName,
        String callback,
        T state,
        Duration dueTime,
        Duration period) {
    return Mono.fromSupplier(() -> {
      if ((callback == null) || callback.isEmpty()) {
        throw new IllegalArgumentException("Timer requires a callback function.");
      }

      String name = timerName;
      if ((timerName == null) || (timerName.isEmpty())) {
        name = String.format("%s_Timer_%d", this.id.toString(), this.timers.size() + 1);
      }

      ActorTimer actorTimer = new ActorTimer(this, name, callback, state, dueTime, period);
      this.timers.put(name, actorTimer);
      return actorTimer;
    }).flatMap(actorTimer -> {
      try {
        return this.actorRuntimeContext.getDaprClient().registerActorTimer(
              this.actorRuntimeContext.getActorTypeInformation().getName(),
              this.id.toString(),
              actorTimer.getName(),
              INTERNAL_SERIALIZER.serialize(actorTimer));
      } catch (Exception e) {
        return Mono.error(e);
      }
    });
  }

  /**
   * Unregisters an Actor timer.
   *
   * @param timerName Name of Timer to be unregistered.
   * @return Asynchronous void response.
   */
  protected Mono<Void> unregisterTimer(String timerName) {
    return Mono.fromSupplier(() -> getActorTimer(timerName))
          .flatMap(actorTimer -> this.actorRuntimeContext.getDaprClient().unregisterActorTimer(
                this.actorRuntimeContext.getActorTypeInformation().getName(),
                this.id.toString(),
                timerName))
          .then(Mono.fromRunnable(() -> this.timers.remove(timerName)));
  }

  /**
   * Unregisters a Reminder.
   *
   * @param reminderName Name of Reminder to be unregistered.
   * @return Asynchronous void response.
   */
  protected Mono<Void> unregisterReminder(String reminderName) {
    return this.actorRuntimeContext.getDaprClient().unregisterActorReminder(
          this.actorRuntimeContext.getActorTypeInformation().getName(),
          this.id.toString(),
          reminderName);
  }

  /**
   * Callback function invoked after an Actor has been activated.
   *
   * @return Asynchronous void response.
   */
  protected Mono<Void> onActivate() {
    return Mono.empty();
  }

  /**
   * Callback function invoked after an Actor has been deactivated.
   *
   * @return Asynchronous void response.
   */
  protected Mono<Void> onDeactivate() {
    return Mono.empty();
  }

  /**
   * Callback function invoked before method is invoked.
   *
   * @param actorMethodContext Method context.
   * @return Asynchronous void response.
   */
  protected Mono<Void> onPreActorMethod(ActorMethodContext actorMethodContext) {
    return Mono.empty();
  }

  /**
   * Callback function invoked after method is invoked.
   *
   * @param actorMethodContext Method context.
   * @return Asynchronous void response.
   */
  protected Mono<Void> onPostActorMethod(ActorMethodContext actorMethodContext) {
    return Mono.empty();
  }

  /**
   * Saves the state of this Actor.
   *
   * @return Asynchronous void response.
   */
  protected Mono<Void> saveState() {
    return this.actorStateManager.save();
  }

  /**
   * Resets the cached state of this Actor.
   */
  void rollback() {
    if (!this.started) {
      throw new IllegalStateException("Cannot reset state before starting call.");
    }

    this.resetState();
    this.started = false;
  }

  /**
   * Resets the cached state of this Actor.
   */
  void resetState() {
    this.actorStateManager.clear();
  }

  /**
   * Gets a given timer by name.
   *
   * @param timerName Timer name.
   * @return Asynchronous void response.
   */
  ActorTimer getActorTimer(String timerName) {
    return timers.getOrDefault(timerName, null);
  }

  /**
   * Internal callback when an Actor is activated.
   *
   * @return Asynchronous void response.
   */
  Mono<Void> onActivateInternal() {
    return Mono.fromRunnable(() -> {
      this.actorTrace.writeInfo(TRACE_TYPE, this.id.toString(), "Activating ...");
      this.resetState();
    }).then(this.onActivate())
          .then(this.doWriteInfo(TRACE_TYPE, this.id.toString(), "Activated"))
          .then(this.saveState());
  }

  /**
   * Internal callback when an Actor is deactivated.
   *
   * @return Asynchronous void response.
   */
  Mono<Void> onDeactivateInternal() {
    this.actorTrace.writeInfo(TRACE_TYPE, this.id.toString(), "Deactivating ...");

    return Mono.fromRunnable(() -> this.resetState())
          .then(this.onDeactivate())
          .then(this.doWriteInfo(TRACE_TYPE, this.id.toString(), "Deactivated"));
  }

  /**
   * Internal callback prior to method be invoked.
   *
   * @param actorMethodContext Method context.
   * @return Asynchronous void response.
   */
  Mono<Void> onPreActorMethodInternal(ActorMethodContext actorMethodContext) {
    return Mono.fromRunnable(() -> {
      if (this.started) {
        throw new IllegalStateException("Cannot invoke a method before completing previous call.");
      }

      this.started = true;
    }).then(this.onPreActorMethod(actorMethodContext));
  }

  /**
   * Internal callback after method is invoked.
   *
   * @param actorMethodContext Method context.
   * @return Asynchronous void response.
   */
  Mono<Void> onPostActorMethodInternal(ActorMethodContext actorMethodContext) {
    return Mono.fromRunnable(() -> {
      if (!this.started) {
        throw new IllegalStateException("Cannot complete a method before starting a call.");
      }
    }).then(this.onPostActorMethod(actorMethodContext))
          .then(this.saveState())
          .then(Mono.fromRunnable(() -> {
            this.started = false;
          }));
  }

  /**
   * Internal method to emit a trace message.
   *
   * @param type    Type of trace message.
   * @param id      Identifier of entity relevant for the trace message.
   * @param message Message to be logged.
   * @return Asynchronous void response.
   */
  private Mono<Void> doWriteInfo(String type, String id, String message) {
    return Mono.fromRunnable(() -> this.actorTrace.writeInfo(type, id, message));
  }

}

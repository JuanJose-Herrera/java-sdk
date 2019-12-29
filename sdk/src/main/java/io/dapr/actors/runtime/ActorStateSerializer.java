/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */
package io.dapr.actors.runtime;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dapr.actors.utils.ObjectSerializer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Duration;

/**
 * Serializes and deserializes an object.
 */
class ActorStateSerializer extends ObjectSerializer {

  /**
   * Shared Json Factory as per Jackson's documentation, used only for this class.
   */
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  /**
   * Shared Json serializer/deserializer as per Jackson's documentation.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Serializes a given state object into byte array.
   *
   * @param state State object to be serialized.
   * @return Array of bytes[] with the serialized content.
   * @throws IOException
   */
  public <T> String serialize(T state) throws IOException {
    if (state == null) {
      return null;
    }

    if (state.getClass() == ActorTimer.class) {
      // Special serializer for this internal classes.
      return serialize((ActorTimer<?>) state);
    }

    if (state.getClass() == ActorReminderParams.class) {
      // Special serializer for this internal classes.
      return serialize((ActorReminderParams) state);
    }

    // Is not an special case.
    return super.serialize(state);
  }

  /**
   * {@inheritDoc}
   * Deserializes the byte array into the original object.
   *
   * @param value String to be parsed.
   * @param clazz Type of the object being deserialized.
   * @param <T>   Generic type of the object being deserialized.
   * @return Object of type T.
   * @throws IOException
   */
  @Override
  public <T> T deserialize(String value, Class<T> clazz) throws IOException {
    if (clazz == ActorReminderParams.class) {
      // Special serializer for this internal classes.
      return (T) deserializeActorReminder(value);
    }

    // Is not one the special cases.
    return super.deserialize(value, clazz);
  }

  /**
   * Faster serialization for Actor's timer.
   * @param timer Timer to be serialized.
   * @return JSON String.
   * @throws IOException If cannot generate JSON.
   */
  private static String serialize(ActorTimer<?> timer) throws IOException {
    try (Writer writer = new StringWriter()) {
      JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
      generator.writeStartObject();
      generator.writeStringField("dueTime", DurationUtils.ConvertDurationToDaprFormat(timer.getDueTime()));
      generator.writeStringField("period", DurationUtils.ConvertDurationToDaprFormat(timer.getPeriod()));
      generator.writeEndObject();
      generator.close();
      writer.flush();
      return writer.toString();
    }
  }

  /**
   * Faster serialization for Actor's reminder.
   * @param reminder Reminder to be serialized.
   * @return JSON String.
   * @throws IOException If cannot generate JSON.
   */
  private static String serialize(ActorReminderParams reminder) throws IOException {
    try (Writer writer = new StringWriter()) {
      JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
      generator.writeStartObject();
      generator.writeStringField("dueTime", DurationUtils.ConvertDurationToDaprFormat(reminder.getDueTime()));
      generator.writeStringField("period", DurationUtils.ConvertDurationToDaprFormat(reminder.getPeriod()));
      if (reminder.getData() != null) {
        generator.writeStringField("data", reminder.getData());
      }
      generator.writeEndObject();
      generator.close();
      writer.flush();
      return writer.toString();
    }
  }

  /**
   * Deserializes an Actor Reminder.
   * @param value String to be deserialized.
   * @return Actor Reminder.
   * @throws IOException If cannot parse JSON.
   */
  private static ActorReminderParams deserializeActorReminder(String value) throws IOException {
    if (value == null) {
      return null;
    }

    JsonNode node = OBJECT_MAPPER.readTree(value);
    Duration dueTime = DurationUtils.ConvertDurationFromDaprFormat(node.get("dueTime").asText());
    Duration period = DurationUtils.ConvertDurationFromDaprFormat(node.get("period").asText());
    String data = node.get("data") != null ? node.get("data").asText() : null;

    return new ActorReminderParams(data, dueTime, period);
  }
}

package io.hstream;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;

/** A data structure like json object. */
public class HRecord {

  private Struct delegate;

  public static HRecordBuilder newBuilder() {
    return new HRecordBuilder();
  }

  public HRecord(Struct delegate) {
    this.delegate = delegate;
  }

  public Struct getDelegate() {
    return delegate;
  }

  public String toString() {
    return delegate.toString();
  }

  public String toJsonString() {
    try {
      return JsonFormat.printer().print(delegate);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean contains(String key) {
    return delegate.containsFields(key);
  }

  public boolean getBoolean(String name) {
    return delegate.getFieldsOrThrow(name).getBoolValue();
  }

  public int getInt(String name) {
    return (int) delegate.getFieldsOrThrow(name).getNumberValue();
  }

  public long getLong(String name) {
    return (long) delegate.getFieldsOrThrow(name).getNumberValue();
  }

  public double getDouble(String name) {
    return (double) delegate.getFieldsOrThrow(name).getNumberValue();
  }

  public String getString(String name) {
    return delegate.getFieldsOrThrow(name).getStringValue();
  }

  public HArray getHArray(String name) {
    return new HArray(delegate.getFieldsOrThrow(name).getListValue());
  }

  public HRecord getHRecord(String name) {
    return new HRecord(delegate.getFieldsOrThrow(name).getStructValue());
  }

  public ByteString toByteString() {
    return delegate.toByteString();
  }
}

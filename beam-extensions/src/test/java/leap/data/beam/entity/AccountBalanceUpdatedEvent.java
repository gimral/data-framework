/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package leap.data.beam.entity;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

@org.apache.avro.specific.AvroGenerated
public class AccountBalanceUpdatedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -1152639298100531791L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AccountBalanceUpdatedEvent\",\"namespace\":\"leap.data.beam.entity\",\"fields\":[{\"name\":\"eventId\",\"type\":[\"null\",\"long\"]},{\"name\":\"traceId\",\"type\":[\"null\",\"long\"]},{\"name\":\"type\",\"type\":[\"null\",\"string\"]},{\"name\":\"data\",\"type\":{\"type\":\"record\",\"name\":\"AccountBalance\",\"fields\":[{\"name\":\"acid\",\"type\":[\"null\",\"long\"]},{\"name\":\"previous_balance\",\"type\":[\"null\",\"double\"]},{\"name\":\"balance\",\"type\":[\"null\",\"double\"]}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<AccountBalanceUpdatedEvent> ENCODER =
      new BinaryMessageEncoder<AccountBalanceUpdatedEvent>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<AccountBalanceUpdatedEvent> DECODER =
      new BinaryMessageDecoder<AccountBalanceUpdatedEvent>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<AccountBalanceUpdatedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<AccountBalanceUpdatedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<AccountBalanceUpdatedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<AccountBalanceUpdatedEvent>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this AccountBalanceUpdatedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a AccountBalanceUpdatedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a AccountBalanceUpdatedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static AccountBalanceUpdatedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private Long eventId;
   private Long traceId;
   private CharSequence type;
   private AccountBalance data;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public AccountBalanceUpdatedEvent() {}

  /**
   * All-args constructor.
   * @param eventId The new value for eventId
   * @param traceId The new value for traceId
   * @param type The new value for type
   * @param data The new value for data
   */
  public AccountBalanceUpdatedEvent(Long eventId, Long traceId, CharSequence type, AccountBalance data) {
    this.eventId = eventId;
    this.traceId = traceId;
    this.type = type;
    this.data = data;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return eventId;
    case 1: return traceId;
    case 2: return type;
    case 3: return data;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: eventId = (Long)value$; break;
    case 1: traceId = (Long)value$; break;
    case 2: type = (CharSequence)value$; break;
    case 3: data = (AccountBalance)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'eventId' field.
   * @return The value of the 'eventId' field.
   */
  public Long getEventId() {
    return eventId;
  }


  /**
   * Sets the value of the 'eventId' field.
   * @param value the value to set.
   */
  public void setEventId(Long value) {
    this.eventId = value;
  }

  /**
   * Gets the value of the 'traceId' field.
   * @return The value of the 'traceId' field.
   */
  public Long getTraceId() {
    return traceId;
  }


  /**
   * Sets the value of the 'traceId' field.
   * @param value the value to set.
   */
  public void setTraceId(Long value) {
    this.traceId = value;
  }

  /**
   * Gets the value of the 'type' field.
   * @return The value of the 'type' field.
   */
  public CharSequence getType() {
    return type;
  }


  /**
   * Sets the value of the 'type' field.
   * @param value the value to set.
   */
  public void setType(CharSequence value) {
    this.type = value;
  }

  /**
   * Gets the value of the 'data' field.
   * @return The value of the 'data' field.
   */
  public AccountBalance getData() {
    return data;
  }


  /**
   * Sets the value of the 'data' field.
   * @param value the value to set.
   */
  public void setData(AccountBalance value) {
    this.data = value;
  }

  /**
   * Creates a new AccountBalanceUpdatedEvent RecordBuilder.
   * @return A new AccountBalanceUpdatedEvent RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new AccountBalanceUpdatedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new AccountBalanceUpdatedEvent RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new AccountBalanceUpdatedEvent RecordBuilder by copying an existing AccountBalanceUpdatedEvent instance.
   * @param other The existing instance to copy.
   * @return A new AccountBalanceUpdatedEvent RecordBuilder
   */
  public static Builder newBuilder(AccountBalanceUpdatedEvent other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for AccountBalanceUpdatedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AccountBalanceUpdatedEvent>
    implements org.apache.avro.data.RecordBuilder<AccountBalanceUpdatedEvent> {

    private Long eventId;
    private Long traceId;
    private CharSequence type;
    private AccountBalance data;
    private AccountBalance.Builder dataBuilder;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.eventId)) {
        this.eventId = data().deepCopy(fields()[0].schema(), other.eventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.data)) {
        this.data = data().deepCopy(fields()[3].schema(), other.data);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (other.hasDataBuilder()) {
        this.dataBuilder = AccountBalance.newBuilder(other.getDataBuilder());
      }
    }

    /**
     * Creates a Builder by copying an existing AccountBalanceUpdatedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(AccountBalanceUpdatedEvent other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.eventId)) {
        this.eventId = data().deepCopy(fields()[0].schema(), other.eventId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.traceId)) {
        this.traceId = data().deepCopy(fields()[1].schema(), other.traceId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.type)) {
        this.type = data().deepCopy(fields()[2].schema(), other.type);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.data)) {
        this.data = data().deepCopy(fields()[3].schema(), other.data);
        fieldSetFlags()[3] = true;
      }
      this.dataBuilder = null;
    }

    /**
      * Gets the value of the 'eventId' field.
      * @return The value.
      */
    public Long getEventId() {
      return eventId;
    }


    /**
      * Sets the value of the 'eventId' field.
      * @param value The value of 'eventId'.
      * @return This builder.
      */
    public Builder setEventId(Long value) {
      validate(fields()[0], value);
      this.eventId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'eventId' field has been set.
      * @return True if the 'eventId' field has been set, false otherwise.
      */
    public boolean hasEventId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'eventId' field.
      * @return This builder.
      */
    public Builder clearEventId() {
      eventId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'traceId' field.
      * @return The value.
      */
    public Long getTraceId() {
      return traceId;
    }


    /**
      * Sets the value of the 'traceId' field.
      * @param value The value of 'traceId'.
      * @return This builder.
      */
    public Builder setTraceId(Long value) {
      validate(fields()[1], value);
      this.traceId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'traceId' field has been set.
      * @return True if the 'traceId' field has been set, false otherwise.
      */
    public boolean hasTraceId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'traceId' field.
      * @return This builder.
      */
    public Builder clearTraceId() {
      traceId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'type' field.
      * @return The value.
      */
    public CharSequence getType() {
      return type;
    }


    /**
      * Sets the value of the 'type' field.
      * @param value The value of 'type'.
      * @return This builder.
      */
    public Builder setType(CharSequence value) {
      validate(fields()[2], value);
      this.type = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'type' field has been set.
      * @return True if the 'type' field has been set, false otherwise.
      */
    public boolean hasType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'type' field.
      * @return This builder.
      */
    public Builder clearType() {
      type = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'data' field.
      * @return The value.
      */
    public AccountBalance getData() {
      return data;
    }


    /**
      * Sets the value of the 'data' field.
      * @param value The value of 'data'.
      * @return This builder.
      */
    public Builder setData(AccountBalance value) {
      validate(fields()[3], value);
      this.dataBuilder = null;
      this.data = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'data' field has been set.
      * @return True if the 'data' field has been set, false otherwise.
      */
    public boolean hasData() {
      return fieldSetFlags()[3];
    }

    /**
     * Gets the Builder instance for the 'data' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public AccountBalance.Builder getDataBuilder() {
      if (dataBuilder == null) {
        if (hasData()) {
          setDataBuilder(AccountBalance.newBuilder(data));
        } else {
          setDataBuilder(AccountBalance.newBuilder());
        }
      }
      return dataBuilder;
    }

    /**
     * Sets the Builder instance for the 'data' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public Builder setDataBuilder(AccountBalance.Builder value) {
      clearData();
      dataBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'data' field has an active Builder instance
     * @return True if the 'data' field has an active Builder instance
     */
    public boolean hasDataBuilder() {
      return dataBuilder != null;
    }

    /**
      * Clears the value of the 'data' field.
      * @return This builder.
      */
    public Builder clearData() {
      data = null;
      dataBuilder = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AccountBalanceUpdatedEvent build() {
      try {
        AccountBalanceUpdatedEvent record = new AccountBalanceUpdatedEvent();
        record.eventId = fieldSetFlags()[0] ? this.eventId : (Long) defaultValue(fields()[0]);
        record.traceId = fieldSetFlags()[1] ? this.traceId : (Long) defaultValue(fields()[1]);
        record.type = fieldSetFlags()[2] ? this.type : (CharSequence) defaultValue(fields()[2]);
        if (dataBuilder != null) {
          try {
            record.data = this.dataBuilder.build();
          } catch (org.apache.avro.AvroMissingFieldException e) {
            e.addParentField(record.getSchema().getField("data"));
            throw e;
          }
        } else {
          record.data = fieldSetFlags()[3] ? this.data : (AccountBalance) defaultValue(fields()[3]);
        }
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<AccountBalanceUpdatedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<AccountBalanceUpdatedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<AccountBalanceUpdatedEvent>
    READER$ = (org.apache.avro.io.DatumReader<AccountBalanceUpdatedEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    if (this.eventId == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeLong(this.eventId);
    }

    if (this.traceId == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeLong(this.traceId);
    }

    if (this.type == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeString(this.type);
    }

    this.data.customEncode(out);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      if (in.readIndex() != 1) {
        in.readNull();
        this.eventId = null;
      } else {
        this.eventId = in.readLong();
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.traceId = null;
      } else {
        this.traceId = in.readLong();
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.type = null;
      } else {
        this.type = in.readString(this.type instanceof Utf8 ? (Utf8)this.type : null);
      }

      if (this.data == null) {
        this.data = new AccountBalance();
      }
      this.data.customDecode(in);

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          if (in.readIndex() != 1) {
            in.readNull();
            this.eventId = null;
          } else {
            this.eventId = in.readLong();
          }
          break;

        case 1:
          if (in.readIndex() != 1) {
            in.readNull();
            this.traceId = null;
          } else {
            this.traceId = in.readLong();
          }
          break;

        case 2:
          if (in.readIndex() != 1) {
            in.readNull();
            this.type = null;
          } else {
            this.type = in.readString(this.type instanceof Utf8 ? (Utf8)this.type : null);
          }
          break;

        case 3:
          if (this.data == null) {
            this.data = new AccountBalance();
          }
          this.data.customDecode(in);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}











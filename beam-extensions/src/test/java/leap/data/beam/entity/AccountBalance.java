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

@org.apache.avro.specific.AvroGenerated
public class AccountBalance extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2071963306220632523L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AccountBalance\",\"namespace\":\"leap.data.beam.entity\",\"fields\":[{\"name\":\"acid\",\"type\":[\"null\",\"long\"]},{\"name\":\"previous_balance\",\"type\":[\"null\",\"double\"]},{\"name\":\"balance\",\"type\":[\"null\",\"double\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<AccountBalance> ENCODER =
      new BinaryMessageEncoder<AccountBalance>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<AccountBalance> DECODER =
      new BinaryMessageDecoder<AccountBalance>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<AccountBalance> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<AccountBalance> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<AccountBalance> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<AccountBalance>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this AccountBalance to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a AccountBalance from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a AccountBalance instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static AccountBalance fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private Long acid;
   private Double previous_balance;
   private Double balance;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public AccountBalance() {}

  /**
   * All-args constructor.
   * @param acid The new value for acid
   * @param previous_balance The new value for previous_balance
   * @param balance The new value for balance
   */
  public AccountBalance(Long acid, Double previous_balance, Double balance) {
    this.acid = acid;
    this.previous_balance = previous_balance;
    this.balance = balance;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return acid;
    case 1: return previous_balance;
    case 2: return balance;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: acid = (Long)value$; break;
    case 1: previous_balance = (Double)value$; break;
    case 2: balance = (Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'acid' field.
   * @return The value of the 'acid' field.
   */
  public Long getAcid() {
    return acid;
  }


  /**
   * Sets the value of the 'acid' field.
   * @param value the value to set.
   */
  public void setAcid(Long value) {
    this.acid = value;
  }

  /**
   * Gets the value of the 'previous_balance' field.
   * @return The value of the 'previous_balance' field.
   */
  public Double getPreviousBalance() {
    return previous_balance;
  }


  /**
   * Sets the value of the 'previous_balance' field.
   * @param value the value to set.
   */
  public void setPreviousBalance(Double value) {
    this.previous_balance = value;
  }

  /**
   * Gets the value of the 'balance' field.
   * @return The value of the 'balance' field.
   */
  public Double getBalance() {
    return balance;
  }


  /**
   * Sets the value of the 'balance' field.
   * @param value the value to set.
   */
  public void setBalance(Double value) {
    this.balance = value;
  }

  /**
   * Creates a new AccountBalance RecordBuilder.
   * @return A new AccountBalance RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new AccountBalance RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new AccountBalance RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new AccountBalance RecordBuilder by copying an existing AccountBalance instance.
   * @param other The existing instance to copy.
   * @return A new AccountBalance RecordBuilder
   */
  public static Builder newBuilder(AccountBalance other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for AccountBalance instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AccountBalance>
    implements org.apache.avro.data.RecordBuilder<AccountBalance> {

    private Long acid;
    private Double previous_balance;
    private Double balance;

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
      if (isValidValue(fields()[0], other.acid)) {
        this.acid = data().deepCopy(fields()[0].schema(), other.acid);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.previous_balance)) {
        this.previous_balance = data().deepCopy(fields()[1].schema(), other.previous_balance);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.balance)) {
        this.balance = data().deepCopy(fields()[2].schema(), other.balance);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing AccountBalance instance
     * @param other The existing instance to copy.
     */
    private Builder(AccountBalance other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.acid)) {
        this.acid = data().deepCopy(fields()[0].schema(), other.acid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.previous_balance)) {
        this.previous_balance = data().deepCopy(fields()[1].schema(), other.previous_balance);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.balance)) {
        this.balance = data().deepCopy(fields()[2].schema(), other.balance);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'acid' field.
      * @return The value.
      */
    public Long getAcid() {
      return acid;
    }


    /**
      * Sets the value of the 'acid' field.
      * @param value The value of 'acid'.
      * @return This builder.
      */
    public Builder setAcid(Long value) {
      validate(fields()[0], value);
      this.acid = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'acid' field has been set.
      * @return True if the 'acid' field has been set, false otherwise.
      */
    public boolean hasAcid() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'acid' field.
      * @return This builder.
      */
    public Builder clearAcid() {
      acid = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'previous_balance' field.
      * @return The value.
      */
    public Double getPreviousBalance() {
      return previous_balance;
    }


    /**
      * Sets the value of the 'previous_balance' field.
      * @param value The value of 'previous_balance'.
      * @return This builder.
      */
    public Builder setPreviousBalance(Double value) {
      validate(fields()[1], value);
      this.previous_balance = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'previous_balance' field has been set.
      * @return True if the 'previous_balance' field has been set, false otherwise.
      */
    public boolean hasPreviousBalance() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'previous_balance' field.
      * @return This builder.
      */
    public Builder clearPreviousBalance() {
      previous_balance = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'balance' field.
      * @return The value.
      */
    public Double getBalance() {
      return balance;
    }


    /**
      * Sets the value of the 'balance' field.
      * @param value The value of 'balance'.
      * @return This builder.
      */
    public Builder setBalance(Double value) {
      validate(fields()[2], value);
      this.balance = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'balance' field has been set.
      * @return True if the 'balance' field has been set, false otherwise.
      */
    public boolean hasBalance() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'balance' field.
      * @return This builder.
      */
    public Builder clearBalance() {
      balance = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AccountBalance build() {
      try {
        AccountBalance record = new AccountBalance();
        record.acid = fieldSetFlags()[0] ? this.acid : (Long) defaultValue(fields()[0]);
        record.previous_balance = fieldSetFlags()[1] ? this.previous_balance : (Double) defaultValue(fields()[1]);
        record.balance = fieldSetFlags()[2] ? this.balance : (Double) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<AccountBalance>
    WRITER$ = (org.apache.avro.io.DatumWriter<AccountBalance>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<AccountBalance>
    READER$ = (org.apache.avro.io.DatumReader<AccountBalance>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    if (this.acid == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeLong(this.acid);
    }

    if (this.previous_balance == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeDouble(this.previous_balance);
    }

    if (this.balance == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeDouble(this.balance);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      if (in.readIndex() != 1) {
        in.readNull();
        this.acid = null;
      } else {
        this.acid = in.readLong();
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.previous_balance = null;
      } else {
        this.previous_balance = in.readDouble();
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.balance = null;
      } else {
        this.balance = in.readDouble();
      }

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          if (in.readIndex() != 1) {
            in.readNull();
            this.acid = null;
          } else {
            this.acid = in.readLong();
          }
          break;

        case 1:
          if (in.readIndex() != 1) {
            in.readNull();
            this.previous_balance = null;
          } else {
            this.previous_balance = in.readDouble();
          }
          break;

        case 2:
          if (in.readIndex() != 1) {
            in.readNull();
            this.balance = null;
          } else {
            this.balance = in.readDouble();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}











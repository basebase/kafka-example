/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package kafka.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class User extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 7303630060876866543L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"kafka.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":[\"int\",\"null\"]},{\"name\":\"city\",\"type\":[\"string\",\"null\"]},{\"name\":\"score\",\"type\":[\"int\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<User> ENCODER =
      new BinaryMessageEncoder<User>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<User> DECODER =
      new BinaryMessageDecoder<User>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<User> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<User> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<User> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<User>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this User to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a User from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a User instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static User fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.lang.CharSequence name;
  private java.lang.Integer age;
  private java.lang.CharSequence city;
  private java.lang.Integer score;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public User() {}

  /**
   * All-args constructor.
   * @param name The new value for name
   * @param age The new value for age
   * @param city The new value for city
   * @param score The new value for score
   */
  public User(java.lang.CharSequence name, java.lang.Integer age, java.lang.CharSequence city, java.lang.Integer score) {
    this.name = name;
    this.age = age;
    this.city = city;
    this.score = score;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return age;
    case 2: return city;
    case 3: return score;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.CharSequence)value$; break;
    case 1: age = (java.lang.Integer)value$; break;
    case 2: city = (java.lang.CharSequence)value$; break;
    case 3: score = (java.lang.Integer)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }


  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return The value of the 'age' field.
   */
  public java.lang.Integer getAge() {
    return age;
  }


  /**
   * Sets the value of the 'age' field.
   * @param value the value to set.
   */
  public void setAge(java.lang.Integer value) {
    this.age = value;
  }

  /**
   * Gets the value of the 'city' field.
   * @return The value of the 'city' field.
   */
  public java.lang.CharSequence getCity() {
    return city;
  }


  /**
   * Sets the value of the 'city' field.
   * @param value the value to set.
   */
  public void setCity(java.lang.CharSequence value) {
    this.city = value;
  }

  /**
   * Gets the value of the 'score' field.
   * @return The value of the 'score' field.
   */
  public java.lang.Integer getScore() {
    return score;
  }


  /**
   * Sets the value of the 'score' field.
   * @param value the value to set.
   */
  public void setScore(java.lang.Integer value) {
    this.score = value;
  }

  /**
   * Creates a new User RecordBuilder.
   * @return A new User RecordBuilder
   */
  public static kafka.avro.User.Builder newBuilder() {
    return new kafka.avro.User.Builder();
  }

  /**
   * Creates a new User RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new User RecordBuilder
   */
  public static kafka.avro.User.Builder newBuilder(kafka.avro.User.Builder other) {
    if (other == null) {
      return new kafka.avro.User.Builder();
    } else {
      return new kafka.avro.User.Builder(other);
    }
  }

  /**
   * Creates a new User RecordBuilder by copying an existing User instance.
   * @param other The existing instance to copy.
   * @return A new User RecordBuilder
   */
  public static kafka.avro.User.Builder newBuilder(kafka.avro.User other) {
    if (other == null) {
      return new kafka.avro.User.Builder();
    } else {
      return new kafka.avro.User.Builder(other);
    }
  }

  /**
   * RecordBuilder for User instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<User>
    implements org.apache.avro.data.RecordBuilder<User> {

    private java.lang.CharSequence name;
    private java.lang.Integer age;
    private java.lang.CharSequence city;
    private java.lang.Integer score;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(kafka.avro.User.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.age)) {
        this.age = data().deepCopy(fields()[1].schema(), other.age);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.city)) {
        this.city = data().deepCopy(fields()[2].schema(), other.city);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.score)) {
        this.score = data().deepCopy(fields()[3].schema(), other.score);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing User instance
     * @param other The existing instance to copy.
     */
    private Builder(kafka.avro.User other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.age)) {
        this.age = data().deepCopy(fields()[1].schema(), other.age);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.city)) {
        this.city = data().deepCopy(fields()[2].schema(), other.city);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.score)) {
        this.score = data().deepCopy(fields()[3].schema(), other.score);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return name;
    }


    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public kafka.avro.User.Builder setName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public kafka.avro.User.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'age' field.
      * @return The value.
      */
    public java.lang.Integer getAge() {
      return age;
    }


    /**
      * Sets the value of the 'age' field.
      * @param value The value of 'age'.
      * @return This builder.
      */
    public kafka.avro.User.Builder setAge(java.lang.Integer value) {
      validate(fields()[1], value);
      this.age = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'age' field has been set.
      * @return True if the 'age' field has been set, false otherwise.
      */
    public boolean hasAge() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'age' field.
      * @return This builder.
      */
    public kafka.avro.User.Builder clearAge() {
      age = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'city' field.
      * @return The value.
      */
    public java.lang.CharSequence getCity() {
      return city;
    }


    /**
      * Sets the value of the 'city' field.
      * @param value The value of 'city'.
      * @return This builder.
      */
    public kafka.avro.User.Builder setCity(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.city = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'city' field has been set.
      * @return True if the 'city' field has been set, false otherwise.
      */
    public boolean hasCity() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'city' field.
      * @return This builder.
      */
    public kafka.avro.User.Builder clearCity() {
      city = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'score' field.
      * @return The value.
      */
    public java.lang.Integer getScore() {
      return score;
    }


    /**
      * Sets the value of the 'score' field.
      * @param value The value of 'score'.
      * @return This builder.
      */
    public kafka.avro.User.Builder setScore(java.lang.Integer value) {
      validate(fields()[3], value);
      this.score = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'score' field has been set.
      * @return True if the 'score' field has been set, false otherwise.
      */
    public boolean hasScore() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'score' field.
      * @return This builder.
      */
    public kafka.avro.User.Builder clearScore() {
      score = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public User build() {
      try {
        User record = new User();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.age = fieldSetFlags()[1] ? this.age : (java.lang.Integer) defaultValue(fields()[1]);
        record.city = fieldSetFlags()[2] ? this.city : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.score = fieldSetFlags()[3] ? this.score : (java.lang.Integer) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<User>
    WRITER$ = (org.apache.avro.io.DatumWriter<User>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<User>
    READER$ = (org.apache.avro.io.DatumReader<User>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.name);

    if (this.age == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeInt(this.age);
    }

    if (this.city == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeString(this.city);
    }

    if (this.score == null) {
      out.writeIndex(1);
      out.writeNull();
    } else {
      out.writeIndex(0);
      out.writeInt(this.score);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);

      if (in.readIndex() != 0) {
        in.readNull();
        this.age = null;
      } else {
        this.age = in.readInt();
      }

      if (in.readIndex() != 0) {
        in.readNull();
        this.city = null;
      } else {
        this.city = in.readString(this.city instanceof Utf8 ? (Utf8)this.city : null);
      }

      if (in.readIndex() != 0) {
        in.readNull();
        this.score = null;
      } else {
        this.score = in.readInt();
      }

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.name = in.readString(this.name instanceof Utf8 ? (Utf8)this.name : null);
          break;

        case 1:
          if (in.readIndex() != 0) {
            in.readNull();
            this.age = null;
          } else {
            this.age = in.readInt();
          }
          break;

        case 2:
          if (in.readIndex() != 0) {
            in.readNull();
            this.city = null;
          } else {
            this.city = in.readString(this.city instanceof Utf8 ? (Utf8)this.city : null);
          }
          break;

        case 3:
          if (in.readIndex() != 0) {
            in.readNull();
            this.score = null;
          } else {
            this.score = in.readInt();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}











package com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class CopyCreatingPrimitiveConverters {


	/**
	 * ObjectInspectorConverters.
	 * 
	 * Mostly copied from the Hive ObjectInspectorPrimitiveConverters
	 */
	
	  
	  public static class BooleanConverter implements Converter {
		private  PrimitiveObjectInspector inputOI;
	    private SettableBooleanObjectInspector outputOI;

	    public BooleanConverter(PrimitiveObjectInspector inputOI,
	        SettableBooleanObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	      
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getBoolean(input,
	            inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the byte type.
	   */
	  public static class ByteConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableByteObjectInspector outputOI;
	    

	    public ByteConverter(PrimitiveObjectInspector inputOI,
	        SettableByteObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	      
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	   
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getByte(input,inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the short type.
	   */
	  public static class ShortConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableShortObjectInspector outputOI;

	    public ShortConverter(PrimitiveObjectInspector inputOI,
	        SettableShortObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	      
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getShort(input,inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the int type.
	   */
	  public static class IntConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableIntObjectInspector outputOI;

	    public IntConverter(PrimitiveObjectInspector inputOI,
	        SettableIntObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getInt(input,inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the long type.
	   */
	  public static class LongConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableLongObjectInspector outputOI;

	    public LongConverter(PrimitiveObjectInspector inputOI,
	        SettableLongObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getLong(input,
	            inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the float type.
	   */
	  public static class FloatConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableFloatObjectInspector outputOI;

	    public FloatConverter(PrimitiveObjectInspector inputOI,
	        SettableFloatObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getFloat(input,
	            inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  /**
	   * A converter for the double type.
	   */
	  public static class DoubleConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableDoubleObjectInspector outputOI;

	    public DoubleConverter(PrimitiveObjectInspector inputOI,
	        SettableDoubleObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      try {
	        return outputOI.create(PrimitiveObjectInspectorUtils.getDouble(input,inputOI));
	      } catch (NumberFormatException e) {
	        return null;
	      }
	    }
	  }

	  public static class DateConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableDateObjectInspector outputOI;

	    public DateConverter(PrimitiveObjectInspector inputOI,
	        SettableDateObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      return outputOI.create(PrimitiveObjectInspectorUtils.getDate(input,inputOI));
	    }
	  }

	  public static class TimestampConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableTimestampObjectInspector outputOI;
	    boolean intToTimestampInSeconds = false;

	    public TimestampConverter(PrimitiveObjectInspector inputOI,
	        SettableTimestampObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	      
	    }

	    public void setIntToTimestampInSeconds(boolean intToTimestampInSeconds) {
	      this.intToTimestampInSeconds = intToTimestampInSeconds;
	    }
	 
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      return outputOI.create(PrimitiveObjectInspectorUtils.getTimestamp(input,
	          inputOI, intToTimestampInSeconds));
	    }
	  }

	  public static class HiveIntervalYearMonthConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableHiveIntervalYearMonthObjectInspector outputOI;

	    public HiveIntervalYearMonthConverter(PrimitiveObjectInspector inputOI,
	        SettableHiveIntervalYearMonthObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      return outputOI.create(PrimitiveObjectInspectorUtils.getHiveIntervalYearMonth(input, inputOI));
	    }
	  }

	  public static class HiveIntervalDayTimeConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableHiveIntervalDayTimeObjectInspector outputOI;

	    public HiveIntervalDayTimeConverter(PrimitiveObjectInspector inputOI,
	        SettableHiveIntervalDayTimeObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      return outputOI.create(PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(input, inputOI));
	    }
	  }

	  public static class HiveDecimalConverter implements Converter {

	    private PrimitiveObjectInspector inputOI;
	    SettableHiveDecimalObjectInspector outputOI;

	    public HiveDecimalConverter(PrimitiveObjectInspector inputOI,
	        SettableHiveDecimalObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }

	      return outputOI.create(PrimitiveObjectInspectorUtils.getHiveDecimal(input, inputOI));
	    }
	  }

	  public static class BinaryConverter implements Converter{

	    PrimitiveObjectInspector inputOI;
	    SettableBinaryObjectInspector outputOI;

	    public BinaryConverter(PrimitiveObjectInspector inputOI,
	        SettableBinaryObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }

	      return outputOI.create(PrimitiveObjectInspectorUtils.getBinary(input,
	          inputOI));
	    }

	  }

	  /**
	   * A helper class to convert any primitive to Text.
	   */
	  public static class TextConverter implements Converter {
	    private final PrimitiveObjectInspector inputOI;
	    
	    private final ByteStream.Output out = new ByteStream.Output();

	    private static byte[] trueBytes = {'T', 'R', 'U', 'E'};
	    private static byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};

	    public TextConverter(PrimitiveObjectInspector inputOI) {
	      // The output ObjectInspector is writableStringObjectInspector.
	      this.inputOI = inputOI;
	    }

	    public Text convert(Object input) {
	      if (input == null) {
	        return null;
	      }

	      Text t = new Text();
	      switch (inputOI.getPrimitiveCategory()) {
	      case VOID:
	        return null;
	      case BOOLEAN:
	        t.set(((BooleanObjectInspector) inputOI).get(input) ? trueBytes
	            : falseBytes);
	        return t;
	      case BYTE:
	        out.reset();
	        LazyInteger.writeUTF8NoException(out, ((ByteObjectInspector) inputOI).get(input));
	        t.set(out.getData(), 0, out.getLength());
	        return t;
	      case SHORT:
	        out.reset();
	        LazyInteger.writeUTF8NoException(out, ((ShortObjectInspector) inputOI).get(input));
	        t.set(out.getData(), 0, out.getLength());
	        return t;
	      case INT:
	        out.reset();
	        LazyInteger.writeUTF8NoException(out, ((IntObjectInspector) inputOI).get(input));
	        t.set(out.getData(), 0, out.getLength());
	        return t;
	      case LONG:
	        out.reset();
	        LazyLong.writeUTF8NoException(out, ((LongObjectInspector) inputOI).get(input));
	        t.set(out.getData(), 0, out.getLength());
	        return t;
	      case FLOAT:
	        t.set(String.valueOf(((FloatObjectInspector) inputOI).get(input)));
	        return t;
	      case DOUBLE:
	        t.set(String.valueOf(((DoubleObjectInspector) inputOI).get(input)));
	        return t;
	      case STRING:
	        if (inputOI.preferWritable()) {
	          t.set(((StringObjectInspector) inputOI).getPrimitiveWritableObject(input));
	        } else {
	          t.set(((StringObjectInspector) inputOI).getPrimitiveJavaObject(input));
	        }
	        return t;
	      case CHAR:
	        // when converting from char, the value should be stripped of any trailing spaces.
	        if (inputOI.preferWritable()) {
	          // char text value is already stripped of trailing space
	          t.set(((HiveCharObjectInspector) inputOI).getPrimitiveWritableObject(input)
	              .getStrippedValue());
	        } else {
	          t.set(((HiveCharObjectInspector) inputOI).getPrimitiveJavaObject(input).getStrippedValue());
	        }
	        return t;
	      case VARCHAR:
	        if (inputOI.preferWritable()) {
	          t.set(((HiveVarcharObjectInspector) inputOI).getPrimitiveWritableObject(input)
	              .toString());
	        } else {
	          t.set(((HiveVarcharObjectInspector) inputOI).getPrimitiveJavaObject(input).toString());
	        }
	        return t;
	      case DATE:
	        t.set(((DateObjectInspector) inputOI).getPrimitiveWritableObject(input).toString());
	        return t;
	      case TIMESTAMP:
	        t.set(((TimestampObjectInspector) inputOI)
	            .getPrimitiveWritableObject(input).toString());
	        return t;
	      case INTERVAL_YEAR_MONTH:
	        t.set(((HiveIntervalYearMonthObjectInspector) inputOI)
	            .getPrimitiveWritableObject(input).toString());
	        return t;
	      case INTERVAL_DAY_TIME:
	        t.set(((HiveIntervalDayTimeObjectInspector) inputOI)
	            .getPrimitiveWritableObject(input).toString());
	        return t;
	      case BINARY:
	        BinaryObjectInspector binaryOI = (BinaryObjectInspector) inputOI;
	        if (binaryOI.preferWritable()) {
	          BytesWritable bytes = binaryOI.getPrimitiveWritableObject(input);
	          t.set(bytes.getBytes(), 0, bytes.getLength());
	        } else {
	          t.set(binaryOI.getPrimitiveJavaObject(input));
	        }
	        return t;
	      case DECIMAL:
	        t.set(((HiveDecimalObjectInspector) inputOI).getPrimitiveWritableObject(input).toString());
	        return t;
	      default:
	        throw new RuntimeException("Hive 2 Internal error: type = " + inputOI.getTypeName());
	      }
	    }
	  }

	  /**
	   * A helper class to convert any primitive to String.
	   */
	  public static class StringConverter implements Converter {
	    PrimitiveObjectInspector inputOI;

	    public StringConverter(PrimitiveObjectInspector inputOI) {
	      // The output ObjectInspector is writableStringObjectInspector.
	      this.inputOI = inputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      return PrimitiveObjectInspectorUtils.getString(input, inputOI);
	    }
	  }


	  public static class HiveVarcharConverter implements Converter {

	    PrimitiveObjectInspector inputOI;
	    SettableHiveVarcharObjectInspector outputOI;
	

	    public HiveVarcharConverter(PrimitiveObjectInspector inputOI,
	        SettableHiveVarcharObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;

	     
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      switch (inputOI.getPrimitiveCategory()) {
	        case BOOLEAN:
	          return outputOI.create( ((BooleanObjectInspector) inputOI).get(input) ?
	                  new HiveVarchar("TRUE", -1) : new HiveVarchar("FALSE", -1));
	        default:
	          return outputOI.create(PrimitiveObjectInspectorUtils.getHiveVarchar(input, inputOI));
	      }
	    }

	  }

	  public static class HiveCharConverter implements Converter {
	    PrimitiveObjectInspector inputOI;
	    SettableHiveCharObjectInspector outputOI;

	    public HiveCharConverter(PrimitiveObjectInspector inputOI,
	        SettableHiveCharObjectInspector outputOI) {
	      this.inputOI = inputOI;
	      this.outputOI = outputOI;
	    }

	    @Override
	    public Object convert(Object input) {
	      if (input == null) {
	        return null;
	      }
	      switch (inputOI.getPrimitiveCategory()) {
	      case BOOLEAN:
	        return outputOI.create(((BooleanObjectInspector) inputOI).get(input) ?
	                new HiveChar("TRUE", -1) : new HiveChar("FALSE", -1));
	      default:
	        return outputOI.create(PrimitiveObjectInspectorUtils.getHiveChar(input, inputOI));
	      }
	    }
	  }
	

	
}

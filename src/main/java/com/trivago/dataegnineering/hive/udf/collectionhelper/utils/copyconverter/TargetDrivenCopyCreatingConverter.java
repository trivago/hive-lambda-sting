package com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;

/**
 * ObjectInspectorConverters.
 * 
 * Mostly copied from the Hive ObjectInspectorConverters
 * 
 */
public final class TargetDrivenCopyCreatingConverter {

	
  /**
   * Returns a converter that converts objects from one OI to another OI. The
   * returned (converted) object belongs to this converter, so that it can be
   * reused across different calls.
   */
  public static Converter getConverter(ObjectInspector inputOI,
      ObjectInspector outputOI) {
    // If the inputOI is the same as the outputOI, just return an
    // IdentityConverter.

    switch (outputOI.getCategory()) {
    case PRIMITIVE:
      return getConverter((PrimitiveObjectInspector) inputOI, (PrimitiveObjectInspector) outputOI);
    case STRUCT:
      return new TargetDrivenStructConverter(inputOI,
          (SettableStructObjectInspector) outputOI);
    case LIST:
      return new ListConverter(inputOI,
          (SettableListObjectInspector) outputOI);
    case MAP:
      return new MapConverter(inputOI,
          (SettableMapObjectInspector) outputOI);
    case UNION:
      return new UnionConverter(inputOI,
          (SettableUnionObjectInspector) outputOI);
    default:
      throw new RuntimeException("Hive internal error: conversion of "
          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
          + " not supported yet.");
    }
  }
  
  
  
  private static Converter getConverter(PrimitiveObjectInspector inputOI,
	      PrimitiveObjectInspector outputOI) {
	    switch (outputOI.getPrimitiveCategory()) {
	    case BOOLEAN:
	      return new CopyCreatingPrimitiveConverters.BooleanConverter(
	          inputOI,
	          (SettableBooleanObjectInspector) outputOI);
	    case BYTE:
	      return new CopyCreatingPrimitiveConverters.ByteConverter(
	          inputOI,
	          (SettableByteObjectInspector) outputOI);
	    case SHORT:
	      return new CopyCreatingPrimitiveConverters.ShortConverter(
	          inputOI,
	          (SettableShortObjectInspector) outputOI);
	    case INT:
	      return new CopyCreatingPrimitiveConverters.IntConverter(
	          inputOI,
	          (SettableIntObjectInspector) outputOI);
	    case LONG:
	      return new CopyCreatingPrimitiveConverters.LongConverter(
	          inputOI,
	          (SettableLongObjectInspector) outputOI);
	    case FLOAT:
	      return new CopyCreatingPrimitiveConverters.FloatConverter(
	          inputOI,
	          (SettableFloatObjectInspector) outputOI);
	    case DOUBLE:
	      return new CopyCreatingPrimitiveConverters.DoubleConverter(
	          inputOI,
	          (SettableDoubleObjectInspector) outputOI);
	    case STRING:
	      if (outputOI instanceof WritableStringObjectInspector) {
	        return new CopyCreatingPrimitiveConverters.TextConverter(
	            inputOI);
	      } else if (outputOI instanceof JavaStringObjectInspector) {
	        return new CopyCreatingPrimitiveConverters.StringConverter(
	            inputOI);
	      }
	    case CHAR:
	      return new CopyCreatingPrimitiveConverters.HiveCharConverter(
	          inputOI,
	          (SettableHiveCharObjectInspector) outputOI);
	    case VARCHAR:
	      return new CopyCreatingPrimitiveConverters.HiveVarcharConverter(
	          inputOI,
	          (SettableHiveVarcharObjectInspector) outputOI);
	    case DATE:
	      return new CopyCreatingPrimitiveConverters.DateConverter(
	          inputOI,
	          (SettableDateObjectInspector) outputOI);
	    case TIMESTAMP:
	      return new CopyCreatingPrimitiveConverters.TimestampConverter(
	          inputOI,
	          (SettableTimestampObjectInspector) outputOI);
	    case INTERVAL_YEAR_MONTH:
	      return new CopyCreatingPrimitiveConverters.HiveIntervalYearMonthConverter(
	          inputOI,
	          (SettableHiveIntervalYearMonthObjectInspector) outputOI);
	    case INTERVAL_DAY_TIME:
	      return new CopyCreatingPrimitiveConverters.HiveIntervalDayTimeConverter(
	          inputOI,
	          (SettableHiveIntervalDayTimeObjectInspector) outputOI);
	    case BINARY:
	      return new CopyCreatingPrimitiveConverters.BinaryConverter(
	          inputOI,
	          (SettableBinaryObjectInspector)outputOI);
	    case DECIMAL:
	      return new CopyCreatingPrimitiveConverters.HiveDecimalConverter(
	          inputOI,
	          (SettableHiveDecimalObjectInspector) outputOI);
	    default:
	      throw new RuntimeException("Hive internal error: conversion of "
	          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
	          + " not supported yet.");
	    }
	  }

  


  /**
   * A converter class for List.
   */
  public static class ListConverter implements Converter {

    private ListObjectInspector inputOI;
    private SettableListObjectInspector outputOI;

 
    private Converter elementConverter;

    public ListConverter(ObjectInspector inputOI,
        SettableListObjectInspector outputOI) {
      if (inputOI instanceof ListObjectInspector) {
        this.inputOI = (ListObjectInspector)inputOI;
        this.outputOI = outputOI;
        elementConverter = getConverter(this.inputOI.getListElementObjectInspector(),
        								outputOI.getListElementObjectInspector());
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
 
      // Convert the elements
      Object output = outputOI.create(inputOI.getListLength(input));
      for (int index = 0; index < inputOI.getListLength(input); index++) {
        Object inputElement = inputOI.getListElement(input, index);
        Object outputElement = elementConverter.convert(
            inputElement);
        outputOI.set(output, index, outputElement);
      }
      return output;
    }

  }

  /**
   * A converter class for Struct.
   */
  public static class TargetDrivenStructConverter implements Converter {

    private StructObjectInspector inputOI;
    private SettableStructObjectInspector outputOI;

    
    private Map<String,StructField> inputFields = new HashMap<>();
    private Map<String,StructField> outputFields = new HashMap<>();

    private Map<String,Converter> fieldConverters = new HashMap<>();



    public TargetDrivenStructConverter(ObjectInspector inputOI,
        SettableStructObjectInspector outputOI) {
      if (inputOI instanceof StructObjectInspector) {
        this.inputOI = (StructObjectInspector)inputOI;
        this.outputOI = outputOI;
        for(StructField f : outputOI.getAllStructFieldRefs()) {
        	StructField inputStructField = this.inputOI.getStructFieldRef(f.getFieldName());
        	outputFields.put(f.getFieldName(), f);	
        	inputFields.put(f.getFieldName(), inputStructField);
        	fieldConverters.put(f.getFieldName(), getConverter(inputStructField.getFieldObjectInspector()
        				, f.getFieldObjectInspector()));
        }
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object output = outputOI.create();

      for(Entry<String,StructField> fieldToCopy : outputFields.entrySet()) {
    	  outputOI.setStructFieldData(output, fieldToCopy.getValue(), 
    			 fieldConverters.get(fieldToCopy.getKey()).convert(inputOI.getStructFieldData(input, inputFields.get(fieldToCopy.getKey()))));	
      }

      return output;
    }
  }

  /**
   * A converter class for Union.
   */
  public static class UnionConverter implements Converter {

    UnionObjectInspector inputOI;
    SettableUnionObjectInspector outputOI;

    // Object inspectors for the tags for the input and output unionss
    List<? extends ObjectInspector> inputTagsOIs;
    List<? extends ObjectInspector> outputTagsOIs;

    ArrayList<Converter> fieldConverters;

    public UnionConverter(ObjectInspector inputOI,
        SettableUnionObjectInspector outputOI) {
      if (inputOI instanceof UnionObjectInspector) {
        this.inputOI = (UnionObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputTagsOIs = this.inputOI.getObjectInspectors();
        outputTagsOIs = outputOI.getObjectInspectors();

        // If the output has some extra fields, set them to NULL in convert().
        int minFields = Math.min(inputTagsOIs.size(), outputTagsOIs.size());
        fieldConverters = new ArrayList<Converter>(minFields);
        for (int f = 0; f < minFields; f++) {
          fieldConverters.add(getConverter(inputTagsOIs.get(f), outputTagsOIs.get(f)));
        }

      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      Object output = outputOI.create();
      
      Object inputFieldValue = inputOI.getField(input);
      Object inputFieldTag = inputOI.getTag(input);
      Object outputFieldValue = null;

      int inputFieldTagIndex = ((Byte)inputFieldTag).intValue();

      if (inputFieldTagIndex >= 0 && inputFieldTagIndex < fieldConverters.size()) {
         outputFieldValue = fieldConverters.get(inputFieldTagIndex).convert(inputFieldValue);
      }

      outputOI.addField(output, outputFieldValue);

      return output;
    }
  }

  /**
   * A converter class for Map.
   */
  public static class MapConverter implements Converter {

    MapObjectInspector inputOI;
    SettableMapObjectInspector outputOI;

    ObjectInspector inputKeyOI;
    ObjectInspector outputKeyOI;

    ObjectInspector inputValueOI;
    ObjectInspector outputValueOI;

    private Converter keyConverter;
    private Converter valueConverter;

    public MapConverter(ObjectInspector inputOI,
        SettableMapObjectInspector outputOI) {
      if (inputOI instanceof MapObjectInspector) {
        this.inputOI = (MapObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputKeyOI = this.inputOI.getMapKeyObjectInspector();
        outputKeyOI = outputOI.getMapKeyObjectInspector();
        inputValueOI = this.inputOI.getMapValueObjectInspector();
        outputValueOI = outputOI.getMapValueObjectInspector();
        keyConverter = getConverter(inputKeyOI, outputKeyOI);
        valueConverter = getConverter(inputValueOI, outputValueOI);
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      Map<?, ?> map = inputOI.getMap(input);
  
      // CLear the output
      Object output = outputOI.create();

      // Convert the key/value pairs
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object inputKey = entry.getKey();
        Object inputValue = entry.getValue();
        Object outputKey = keyConverter.convert(inputKey);
        Object outputValue = valueConverter.convert(inputValue);
        outputOI.put(output, outputKey, outputValue);
      }
      return output;
    }

  }

}

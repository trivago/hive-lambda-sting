package com.trivago.dataegnineering.hive.udf.collectionhelper.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMacro;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.IdentityConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter.TargetDrivenCopyCreatingConverter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public interface GenericUDFParamBridge {

	public ObjectInspector[] getOis();
	public DeferredObject[] deferr(DeferredObject[] toDefer);
	
	public static class NoOpBridge implements GenericUDFParamBridge {

		public NoOpBridge(ObjectInspector[] ois) {
			super();
			this.ois = Arrays.copyOf(ois, ois.length);
		}

		private   ObjectInspector[] ois;
		
		@Override
		public ObjectInspector[] getOis() {
			return this.ois = Arrays.copyOf(ois, ois.length);
		}

		@Override
		public DeferredObject[] deferr(DeferredObject[] toDefer) {
			return toDefer;
		}
		
	}
	
	public static class UnAndRepackBridge implements GenericUDFParamBridge {
	
	private   ObjectInspector[] ois;
	private   Converter[] converters;
	private   ConvertedDeferredObject[] deferred;
	
		public UnAndRepackBridge(ObjectInspector[] ois, Converter[] converters) {
			super();
			this.ois = Arrays.copyOf(ois, ois.length);
			this.converters = Arrays.copyOf(converters, converters.length);
			this.deferred = new ConvertedDeferredObject[ois.length];
		}
	
		@Override
		public ObjectInspector[] getOis() {
			return this.ois = Arrays.copyOf(ois, ois.length);
		}

		@Override
		@SuppressFBWarnings(justification = "this part is executed per row",value ="EI_EXPOSE_REP" )
		public DeferredObject[] deferr(DeferredObject[] toDefer) {
			assert(toDefer.length == converters.length);
			for(int i = 0;i < toDefer.length; i++) {
				deferred[i] = new ConvertedDeferredObject(converters[i], toDefer[i]); //todo cache;
			}
			return deferred;
		}
		
		public static class ConvertedDeferredObject implements DeferredObject{

			public ConvertedDeferredObject(Converter c, DeferredObject d) {
				super();
				this.c = c;
				this.d = d;
			}

			private Converter c;
			private DeferredObject d;
			
			@Override
			public void prepare(int version) throws HiveException {}

			@Override
			public Object get() throws HiveException {
				return c.convert(d.get());
			}
			
		}
	}

	
	public static GenericUDFParamBridge getParameterInspectors(GenericUDF macro, ObjectInspector[] originallyPlanned) throws UDFArgumentException {
		Converter[] converters = new Converter[originallyPlanned.length];
		Arrays.fill(converters,new IdentityConverter());
		if(macro instanceof GenericUDFMacro) {
			List<TypeInfo> types =  ((GenericUDFMacro) macro).getColTypes();
			if(types.size() != originallyPlanned.length) {
				throw new UDFArgumentException("planned for " + Arrays.toString(originallyPlanned) + " but had " + types.toString() + 
												" count type " + types.size() + " count planned " + originallyPlanned.length );
			}
			ObjectInspector[] resultOis = new ObjectInspector[types.size()];
			
			for(int i = 0; i < resultOis.length ; i++) {
				resultOis[i] = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(types.get(i));
				converters[i] = TargetDrivenCopyCreatingConverter.getConverter(originallyPlanned[i], resultOis[i]);
			}
			return new UnAndRepackBridge(resultOis, converters);
		}
		return new NoOpBridge(originallyPlanned);
		
	}


	

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cfzq.hive.udf.generic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class XSSGenericUDAFOLCollectionEvaluator extends GenericUDAFEvaluator
    implements Serializable {

  private static final long serialVersionUID = 1l;
  private transient Object[] ret;

  enum BufferType { SET, LIST, MAP }

  // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
  private transient ObjectInspector inputOI;
  private PrimitiveObjectInspector inputOI2;
  private PrimitiveObjectInspector inputOI3;
  private StructField sf1;
  private StructField sf2;
  private StructField sf3;
  private StandardListObjectInspector valueListOI;
  // private boolean asc = true;
  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
  // of objs)
  private transient StandardListObjectInspector loi;

  private transient StandardStructObjectInspector internalMergeOI;
  // private transient PrimitiveObjectInspector internalMergeOI2;

  private BufferType bufferType;

  //needed by kyro
  public XSSGenericUDAFOLCollectionEvaluator() {
  }

  public XSSGenericUDAFOLCollectionEvaluator(BufferType bufferType){
    this.bufferType = bufferType;
  }

  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    super.init(m, parameters);
    // init output object inspectors
    // The output of a partial aggregation is a list
    if (m == Mode.PARTIAL1) {
      inputOI = parameters[0];
      inputOI2 = (PrimitiveObjectInspector) parameters[1];
      if (parameters.length == 3) {
    	  	inputOI3 = (PrimitiveObjectInspector) parameters[2];
      }
      List<String> lNames = new ArrayList<String>();
      lNames.add("asc");
      lNames.add("keyList");
      lNames.add("valueList");
      List<ObjectInspector> lInspector = new ArrayList<ObjectInspector>();
      lInspector.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
      lInspector.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector));
      lInspector.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI)));
      return ObjectInspectorFactory.getStandardStructObjectInspector(lNames, lInspector);
//      return ObjectInspectorFactory.getStandardMapObjectInspector(
//    		  PrimitiveObjectInspectorFactory.writableStringObjectInspector,
//          ObjectInspectorUtils.getStandardObjectInspector(inputOI));
    } else {
      if (parameters.length >= 2) {
        //no map aggregation.
        inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        inputOI2 = (PrimitiveObjectInspector) parameters[1];
        if (parameters.length == 3) {
      	  	inputOI3 = (PrimitiveObjectInspector) parameters[2];
        }
        return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
      } else {
        internalMergeOI = (StandardStructObjectInspector) parameters[0];
        // internalMergeOI2 = (PrimitiveObjectInspector) parameters[1];
        sf1 = internalMergeOI.getStructFieldRef("asc");
        sf2 = internalMergeOI.getStructFieldRef("keyList");
        sf3 = internalMergeOI.getStructFieldRef("valueList");
        // valueListOI = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(sf3.getFieldObjectInspector());
        inputOI = ((StandardListObjectInspector)sf3.getFieldObjectInspector()).getListElementObjectInspector();
        inputOI2 = (PrimitiveObjectInspector) ((StandardListObjectInspector)sf2.getFieldObjectInspector()).getListElementObjectInspector();
        // keyOI = ((StandardMapObjectInspector)sf2.getFieldObjectInspector()).getMapKeyObjectInspector();
        // loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
        loi = ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
        return loi;
      }
    }
  }


  class MkArrayAggregationBuffer extends AbstractAggregationBuffer {
	public boolean asc;
    public ArrayList<Text> keyList;
    public ArrayList<Object> valueList;

    public MkArrayAggregationBuffer() {
    		keyList = new ArrayList<Text>();
    		valueList = new ArrayList<Object>();
    	  	asc = true;
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
	   ((MkArrayAggregationBuffer) agg).keyList.clear();
	   ((MkArrayAggregationBuffer) agg).valueList.clear();
	   ((MkArrayAggregationBuffer) agg).asc = true;
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
    return ret;
  }

  //mapside
  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters)
      throws HiveException {
    assert (parameters.length == 2 || parameters.length == 3);
    Object p = parameters[0];
    String key = PrimitiveObjectInspectorUtils.getString(parameters[1], inputOI2);

    if (p != null) {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      
    	  if (parameters.length == 3) {
    	  	  myagg.asc = PrimitiveObjectInspectorUtils.getBoolean(parameters[2], inputOI3);
      }
      putIntoCollection(p, new Text(key), myagg);
    }
  }

  //mapside
  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    ret = new Object[3];
//    Map<Text, Object> ret = new TreeMap<Text, Object>();
//    ret.putAll(myagg.container);
    BooleanWritable asc = new BooleanWritable();
    asc.set(myagg.asc);
    ret[0] = asc;
    ret[1] = myagg.keyList;
    ret[2] = myagg.valueList;
    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial)
      throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    BooleanWritable asc = (BooleanWritable) internalMergeOI.getStructFieldData(partial, sf1);
    LazyBinaryArray keyList = (LazyBinaryArray) internalMergeOI.getStructFieldData(partial, sf2);
    LazyBinaryArray valueList = (LazyBinaryArray) internalMergeOI.getStructFieldData(partial, sf3);
    // List<Object> valueList = (List<Object>) valueListOI.getList(internalMergeOI.getStructFieldData(partial, sf3));
    myagg.asc = asc.get();
	for(int i=0; i<keyList.getListLength(); i++) {
	    putIntoCollection(valueList.getListElementObject(i), (Text) keyList.getListElementObject(i), myagg);
	}
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    TreeMap<Text, Object> tm = null;
    if (myagg.asc) {
    		tm = new TreeMap<Text, Object>();
    } else {
    		tm = new TreeMap<Text, Object>(Collections.reverseOrder());
    }
    for(int i=0; i<myagg.keyList.size(); i++) {
    		/*if (inputOI instanceof StructObjectInspector) {
    			Object pCopy = ObjectInspectorUtils.copyToStandardObject(myagg.valueList.get(i),  inputOI);
    		    tm.put(myagg.keyList.get(i), pCopy);
    		} else*/ {
    			 tm.put(myagg.keyList.get(i), myagg.valueList.get(i));
    		}
    		
	}
    List<Object> ret = new ArrayList<Object>(tm.size());
    for (Map.Entry<Text, Object> entry: tm.entrySet()) {
    		ret.add(entry.getValue());
    }
    return ret;
  }

  private void putIntoCollection(Object p, Text key, MkArrayAggregationBuffer myagg) {
    Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,  this.inputOI);
    myagg.keyList.add(key);
    myagg.valueList.add(pCopy);
  }

  public BufferType getBufferType() {
    return bufferType;
  }

  public void setBufferType(BufferType bufferType) {
    this.bufferType = bufferType;
  }
  
  public static void main(String[] args) {
	  Map<Text, Object> map = new TreeMap<Text, Object>(Collections.reverseOrder());
	  map.put(new Text("20180904"), 1);
	  map.put(new Text("20180903"), 2);
	  map.put(new Text("20180905"), 3);
	  map.put(new Text("20180901"), 4);
	  for (Map.Entry<Text, Object> entry: map.entrySet()) {
		  System.out.println(entry.getKey() + ", " + entry.getValue());
	  }
  }
}

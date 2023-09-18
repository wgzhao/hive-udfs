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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@Description(name = "udaf_collect_order_list", value = "_FUNC_(x) - Returns a list of objects with duplicates")
public class XSSGenericUDAFCollectList extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(XSSGenericUDAFCollectList.class.getName());

  public XSSGenericUDAFCollectList() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2 && parameters.length != 3) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "two or three argument is expected.");
    }

    switch (parameters[0].getCategory()) {
      case PRIMITIVE:
      case STRUCT:
      case MAP:
      case LIST:
        break;
      default:
        throw new UDFArgumentTypeException(0,
            "Only primitive, struct, list or map type arguments are accepted but "
                + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    if (parameters.length == 3 && !parameters[2].equals(TypeInfoFactory.booleanTypeInfo)) {
    		throw new UDFArgumentTypeException(2, "third argument must be a boolean expression");
    }
    return new XSSGenericUDAFOLCollectionEvaluator();
  }
}

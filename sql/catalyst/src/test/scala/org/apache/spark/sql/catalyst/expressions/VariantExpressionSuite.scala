/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.variant.VariantUtil._

class VariantExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("parse_json") {
    def check(json: String, expectedValue: Array[Byte], expectedMetadata: Array[Byte]): Unit = {
      checkEvaluation(ParseJson(Literal(json)), new VariantVal(expectedValue, expectedMetadata))
    }

    // Dictionary size is `0, 0, 0` for value 0. An empty dictionary contains one offset `3, 0, 0`
    // for value 3 because the offset counts from the starting position of the offset list to the
    // string.
    val emptyMetadata = Array[Byte](VERSION, 0, 0, 0, 3, 0, 0)
    check("null", Array(atomicHeader(NULL)), emptyMetadata)
    check("true", Array(atomicHeader(TRUE)), emptyMetadata)
    check("false", Array(atomicHeader(FALSE)), emptyMetadata)
    check("1", Array(atomicHeader(INT1), 1), emptyMetadata)
    check("-1", Array(atomicHeader(INT1), -1), emptyMetadata)
    check("127", Array(atomicHeader(INT1), 127), emptyMetadata)
    check("128", Array(atomicHeader(INT2), -128, 0), emptyMetadata)
    check("-32768", Array(atomicHeader(INT2), 0, -128), emptyMetadata)
    check("-32769", Array(atomicHeader(INT4), -1, 127, -1, -1), emptyMetadata)
    check("2147483647", Array(atomicHeader(INT4), -1, -1, -1, 127), emptyMetadata)
    check("2147483648", Array(atomicHeader(INT8), 0, 0, 0, -128, 0, 0, 0, 0), emptyMetadata)
    check("9223372036854775807",
      Array(atomicHeader(INT8), -1, -1, -1, -1, -1, -1, -1, 127), emptyMetadata)
    check("-9223372036854775808",
      Array(atomicHeader(INT8), 0, 0, 0, 0, 0, 0, 0, -128), emptyMetadata)
    check("9223372036854775808",
      Array(atomicHeader(DECIMAL16), 0, 0, 0, 0, 0, 0, 0, 0, -128, 0, 0, 0, 0, 0, 0, 0, 0),
      emptyMetadata)
    check("1.0", Array(atomicHeader(DECIMAL4), 1, 10, 0, 0, 0), emptyMetadata)
    check("1.01", Array(atomicHeader(DECIMAL4), 2, 101, 0, 0, 0), emptyMetadata)
    check("99999.9999", Array(atomicHeader(DECIMAL4), 4, -1, -55, -102, 59), emptyMetadata)
    check("99999.99999",
      Array(atomicHeader(DECIMAL8), 5, -1, -29, 11, 84, 2, 0, 0, 0), emptyMetadata)
    check("0.000000001", Array(atomicHeader(DECIMAL4), 9, 1, 0, 0, 0), emptyMetadata)
    check("0.0000000001", Array(atomicHeader(DECIMAL8), 10, 1, 0, 0, 0, 0, 0, 0, 0), emptyMetadata)
    check("9" * 38,
      Array[Byte](atomicHeader(DECIMAL16), 0) ++ BigInt("9" * 38).toByteArray.reverse,
      emptyMetadata)
    check("1" + "0" * 38,
      Array(atomicHeader(DOUBLE)) ++
        BigInt(java.lang.Double.doubleToLongBits(1E38)).toByteArray.reverse,
      emptyMetadata)
    check("\"\"", Array(shortStrHeader(0)), emptyMetadata)
    check("\"abcd\"", Array(shortStrHeader(4), 'a', 'b', 'c', 'd'), emptyMetadata)
    check("\"" + ("x" * 63) + "\"",
      Array(shortStrHeader(63)) ++ Array.fill(63)('x'.toByte), emptyMetadata)
    check("\"" + ("y" * 64) + "\"",
      Array[Byte](atomicHeader(LONG_STR), 64, 0, 0) ++ Array.fill(64)('y'.toByte), emptyMetadata)
    check("{}", Array(objectHeader(false, 1, 1),
      /* size */ 0,
      /* offset list */ 0), emptyMetadata)
    check("[]", Array(arrayHeader(false, 1),
      /* size */ 0,
      /* offset list */ 0), emptyMetadata)
    check("""{"a": 1, "b": 2, "c": "3"}""", Array(objectHeader(false, 1, 1),
      /* size */ 3,
      /* id list */ 0, 1, 2,
      /* offset list */ 0, 2, 4, 6,
      /* field data */ atomicHeader(INT1), 1, atomicHeader(INT1), 2, shortStrHeader(1), '3'),
      Array(VERSION, 3, 0, 0, 12, 0, 0, 13, 0, 0, 14, 0, 0, 15, 0, 0, 'a', 'b', 'c'))
    check("""{"z": 1, "y": 2, "x": "3"}""", Array(objectHeader(false, 1, 1),
      /* size */ 3,
      /* id list */ 2, 1, 0,
      /* offset list */ 4, 2, 0, 6,
      /* field data */ atomicHeader(INT1), 1, atomicHeader(INT1), 2, shortStrHeader(1), '3'),
      Array(VERSION, 3, 0, 0, 12, 0, 0, 13, 0, 0, 14, 0, 0, 15, 0, 0, 'z', 'y', 'x'))
    check("""[null, true, {"false" : 0}]""", Array(arrayHeader(false, 1),
      /* size */ 3,
      /* offset list */ 0, 1, 2, 9,
      /* element data */ atomicHeader(NULL), atomicHeader(TRUE), objectHeader(false, 1, 1),
      /* size */ 1,
      /* id list */ 0,
      /* offset list */ 0, 2,
      /* field data */ atomicHeader(INT1), 0),
      Array(VERSION, 1, 0, 0, 6, 0, 0, 11, 0, 0, 'f', 'a', 'l', 's', 'e'))

    checkExceptionInExpression[SparkException](ParseJson(Literal("")),
      "Malformed records are detected in record parsing")
    checkExceptionInExpression[SparkException](ParseJson(Literal("[")),
      "Malformed records are detected in record parsing")

  }

  test("parse_json negative") {
    for (json <- Seq("", "[", "+1", "1a", """{"a": 1, "b": 2, "a": "3"}""")) {
      checkExceptionInExpression[SparkException](ParseJson(Literal(json)),
        "Malformed records are detected in record parsing")
    }
  }
}

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

package org.apache.spark.sql.jdbc

class DB2DatabaseOnDocker extends DatabaseOnDocker {
  override val imageName = sys.env.getOrElse("DB2_DOCKER_IMAGE_NAME", "ibmcom/db2:11.5.8.0")
  override val env = Map(
    "DB2INST1_PASSWORD" -> "rootpass",
    "LICENSE" -> "accept",
    "DBNAME" -> "foo",
    "ARCHIVE_LOGS" -> "false",
    "AUTOCONFIG" -> "false"
  )
  override val usesIpc = false
  override val jdbcPort: Int = 50000
  override val privileged = true
  override def getJdbcUrl(ip: String, port: Int): String =
    s"jdbc:db2://$ip:$port/foo:user=db2inst1;password=rootpass;retrieveMessagesFromServerOnGetMessage=true;" //scalastyle:ignore
}

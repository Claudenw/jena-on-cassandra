/*
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

package org.apache.jena.cassandra.graph;

/**
 * An implementation of the Jena datastore on Cassandra.
 * 
 * This implementation uses 4 tables.  All 4 tables are written to on insert and delete.
 * When querying a single table is used based on the columns provided in the query. 
 * 
 * Graph patterns
 * <ul>
 * <li>SPOG</li>
 * <li>POGS</li>
 * <li>OSGP</li>
 * <li>GSPO</li>
 * </ul>
 * 
 * The mapping of query columns to table is performed in the CassandraConnection class.
 * 
 * <p>
 * All tables have the same structure with different primary key definition.  All primary keys are 
 * multi-segmented with the first segment being the partition key.  The column order in the primary key 
 * match the column names from the table name. (e.g. SPOG has the primary key = subject, predicate, object,
 * graph).
 */


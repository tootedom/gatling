/**
 * Copyright 2011-2012 eBusiness Information, Groupe Excilys (www.excilys.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.excilys.ebi.gatling.jdbc.util

import java.sql.ResultSet

object RowIterator {

	implicit def ResultSet2RowIterator(resultSet: ResultSet) = new RowIterator(resultSet)

}

// TODO : use an implicit class after upgrade to Scala 2.10
class RowIterator(resultSet: ResultSet) extends Iterator[Array[AnyRef]] {

	val columnCount = resultSet.getMetaData.getColumnCount

	def hasNext = !resultSet.isLast

	def next = {
		resultSet.next
		(for (i <- 1 to columnCount) yield (resultSet.getObject(i))).toArray
	}
}

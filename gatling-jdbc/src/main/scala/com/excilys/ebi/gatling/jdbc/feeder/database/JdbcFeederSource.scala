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
package com.excilys.ebi.gatling.jdbc.feeder.database

import java.sql.DriverManager
import java.sql.ResultSet.{ CONCUR_READ_ONLY, TYPE_FORWARD_ONLY }

import com.excilys.ebi.gatling.core.util.IOHelper.use
import com.excilys.ebi.gatling.jdbc.util.RowIterator.ResultSet2RowIterator

object JdbcFeederSource {

	def apply(url: String, username: String, password: String, sql: String): Array[Map[String, String]] = {

		use(DriverManager.getConnection(url, username, password)) { connection =>
			val preparedStatement = connection.prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)
			val resultSet = preparedStatement.executeQuery
			val rsmd = resultSet.getMetaData

			val columnNames = for (i <- 1 to rsmd.getColumnCount) yield rsmd.getColumnName(i)

			resultSet.map(contents => (columnNames.zip(contents.map(_.toString))).toMap).toArray
		}
	}

}
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
package com.excilys.ebi.gatling.core.structure

import java.util.UUID

import com.excilys.ebi.gatling.core.action.builder.{ SimpleActionBuilder, TryMaxActionBuilder }
import com.excilys.ebi.gatling.core.structure.ChainBuilder.emptyChain

trait Errors[B] extends Execs[B] {

	def exitBlockOnFail(chain: ChainBuilder): B = tryMax(1)(chain)
	def tryMax(times: Int)(chain: ChainBuilder): B = tryMax(times, None)(chain)
	def tryMax(times: Int, counterName: String)(chain: ChainBuilder): B = tryMax(times, Some(counterName))(chain)
	private def tryMax(times: Int, counterName: Option[String])(chain: ChainBuilder): B = {

		require(times >= 1, "Can't set up a max try <= 1")

		def buildTransactionalChain(chain: ChainBuilder): ChainBuilder = {
			val startBlock = SimpleActionBuilder(session => session.clearFailed.setMustExitOnFail)
			val endBlock = SimpleActionBuilder(session => session.clearMustExitOnFail)
			emptyChain.exec(startBlock).exec(chain).exec(endBlock)
		}

		val loopCounterName = counterName.getOrElse(UUID.randomUUID.toString)
		exec(TryMaxActionBuilder(times, buildTransactionalChain(chain), loopCounterName))
	}

	def exitHereIfFailed: B = exec(SimpleActionBuilder(session => session.setMustExitOnFail))
}
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
package com.excilys.ebi.gatling.core.action.builder

import com.excilys.ebi.gatling.core.action.{ Bypassable, SimpleAction, system }
import com.excilys.ebi.gatling.core.config.ProtocolConfigurationRegistry
import com.excilys.ebi.gatling.core.session.Session

import akka.actor.{ ActorRef, Props }

object SimpleActionBuilder {

	/**
	 * Creates a simple action builder
	 *
	 * @param sessionFunction the function that will be executed by the built simple action
	 * @param bypassable if the resulting action is to be bypassable
	 */
	def apply(sessionFunction: Session => Session, bypassable: Boolean = false) = new SimpleActionBuilder(sessionFunction, bypassable)
}

/**
 * Builder for SimpleAction
 *
 * @constructor creates a SimpleActionBuilder
 * @param sessionFunction the function that will be executed by the simple action
 */
class SimpleActionBuilder(sessionFunction: Session => Session, bypassable: Boolean) extends ActionBuilder {

	def build(next: ActorRef, protocolConfigurationRegistry: ProtocolConfigurationRegistry) =
		if (bypassable)
			system.actorOf(Props(new SimpleAction(sessionFunction, next) with Bypassable))
		else
			system.actorOf(Props(new SimpleAction(sessionFunction, next)))
}
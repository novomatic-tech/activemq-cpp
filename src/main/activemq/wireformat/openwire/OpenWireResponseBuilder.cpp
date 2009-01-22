/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "OpenWireResponseBuilder.h"

#include <typeinfo>
#include <activemq/commands/ActiveMQBytesMessage.h>
#include <activemq/commands/ActiveMQMapMessage.h>
#include <activemq/commands/ActiveMQMessage.h>
#include <activemq/commands/ActiveMQObjectMessage.h>
#include <activemq/commands/ActiveMQStreamMessage.h>
#include <activemq/commands/ActiveMQTextMessage.h>
#include <activemq/commands/BrokerInfo.h>
#include <activemq/commands/ConnectionInfo.h>
#include <activemq/commands/ConsumerInfo.h>
#include <activemq/commands/DestinationInfo.h>
#include <activemq/commands/ProducerInfo.h>
#include <activemq/commands/Response.h>
#include <activemq/commands/RemoveSubscriptionInfo.h>
#include <activemq/commands/RemoveInfo.h>
#include <activemq/commands/SessionInfo.h>
#include <activemq/commands/ShutdownInfo.h>
#include <activemq/commands/WireFormatInfo.h>

using namespace activemq;
using namespace activemq::wireformat;
using namespace activemq::wireformat::openwire;
using namespace activemq::transport;
using namespace activemq::transport::mock;

////////////////////////////////////////////////////////////////////////////////
Response* OpenWireResponseBuilder::buildResponse(
    const transport::Command* command ){

    if( typeid( *command ) == typeid( commands::ActiveMQBytesMessage ) ||
        typeid( *command ) == typeid( commands::ActiveMQMapMessage ) ||
        typeid( *command ) == typeid( commands::ActiveMQMessage ) ||
        typeid( *command ) == typeid( commands::ActiveMQObjectMessage ) ||
        typeid( *command ) == typeid( commands::ActiveMQStreamMessage ) ||
        typeid( *command ) == typeid( commands::ActiveMQTextMessage ) ||
        typeid( *command ) == typeid( commands::ConnectionInfo ) ||
        typeid( *command ) == typeid( commands::ConsumerInfo ) ||
        typeid( *command ) == typeid( commands::DestinationInfo ) ||
        typeid( *command ) == typeid( commands::ProducerInfo ) ||
        typeid( *command ) == typeid( commands::RemoveSubscriptionInfo ) ||
        typeid( *command ) == typeid( commands::RemoveInfo ) ||
        typeid( *command ) == typeid( commands::SessionInfo ) ) {

        // These Commands just require a response that matches their command IDs
        commands::Response* response = new commands::Response();
        response->setCorrelationId( command->getCommandId() );
        return response;
    }

    // If this command requires a response we don't know what it is
    // so we throw an exception.
    if( command->isResponseRequired() ) {
        throw transport::CommandIOException( __FILE__, __LINE__,
            "OpenWireResponseBuilder - unrecognized command" );
    }

    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
void OpenWireResponseBuilder::buildIncomingCommands(
    const transport::Command* command, decaf::util::Queue<transport::Command*>& queue ){

    // Delegate this to buildResponse
    if( command->isResponseRequired() ) {
        queue.push( buildResponse( command ) );
    }

    if( typeid( *command ) == typeid( commands::WireFormatInfo ) ) {

        // Return a copy of the callers own requested WireFormatInfo
        // so they get exactly the settings they asked for.
        queue.push( command->cloneCommand() );
    }
}

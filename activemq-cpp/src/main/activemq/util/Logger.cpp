#include "Logger.h"

#include <cctype>

#include <decaf/lang/System.h>

using namespace LightBridge::Middleware::Logger;

// small helper function
namespace {
    bool isEnabled(const std::string& variable) {
        std::string env = "";
        try {
            env = decaf::lang::System::getenv(variable);
        } catch (decaf::lang::exceptions::NullPointerException& ex) {
            // variable not set
            return false;
        }
        std::string value;
        value.resize(env.size());
        std::transform(env.begin(), env.end(), value.begin(), tolower);

        return value == "true" || value == "1";
    }
}

decaf::lang::Pointer<LBLogger> activemq::util::Logger::externalLogger;
bool activemq::util::Logger::tracingEnabled;
bool activemq::util::Logger::IOtracingEnabled;


void activemq::util::Logger::initialize(ILogger *logger) {
    if (logger) {
        externalLogger.reset(new LBLogger(logger));
    } else {
        externalLogger.reset(NULL);
    }

    tracingEnabled = isEnabled("NOVOPRIME_ACTIVEMQCPP_TRACE");
    IOtracingEnabled = isEnabled("NOVOPRIME_ACTIVEMQCPP_TRACE_IO");

    log("ActiveMQ logger initialized", LightBridge::Middleware::Logger::LOG_SEV_INFO, "activemq");
    log(std::string("ActiveMQ trace logs ") + (tracingEnabled ? "enabled" : "disabled"), LightBridge::Middleware::Logger::LOG_SEV_INFO, "activemq");
    log(std::string("ActiveMQ IO trace logs ") + (IOtracingEnabled ? "enabled" : "disabled"), LightBridge::Middleware::Logger::LOG_SEV_INFO, "activemq");
}

void activemq::util::Logger::log(const std::string &entry, LightBridge::Middleware::Logger::Severity severity, const std::string &category) {
    if (externalLogger != NULL) {
        externalLogger->Log(entry, severity, category);
    }
}

void activemq::util::Logger::log(const std::string &entry, LightBridge::Middleware::Logger::Severity severity, const std::vector<std::string> &categories) {
    if (externalLogger != NULL) {
        externalLogger->Log(entry, severity, categories);
    }
}

void activemq::util::Logger::trace(const std::string &entry, const std::string &category) {
    if (tracingEnabled && externalLogger != NULL) {
        std::vector<std::string> categories_;
        categories_.push_back(category);
        categories_.push_back("trace");
        externalLogger->Log(entry, LOG_SEV_DEBUG, categories_);
    }
}

void activemq::util::Logger::trace(const std::string &entry, const std::vector<std::string> &categories) {
    if (tracingEnabled && externalLogger != NULL) {
        std::vector<std::string> categories_ = categories;
        categories_.push_back("trace");
        externalLogger->Log(entry, LOG_SEV_DEBUG, categories_);
    }
}

void activemq::util::Logger::traceIO(std::string const &entry, std::vector<std::string> const &categories) {
    if (IOtracingEnabled && externalLogger != NULL) {
        std::vector<std::string> categories_ = categories;
        categories_.push_back("trace_io");
        externalLogger->Log(entry, LOG_SEV_DEBUG, categories_);
    }
}

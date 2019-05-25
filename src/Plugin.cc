
#include "Plugin.h"
#include "MySQLWriter.h"

namespace plugin { namespace WingPig_MySQL { Plugin plugin; } }

using namespace plugin::WingPig_MySQL;

plugin::Configuration Plugin::Configure()
	{
	AddComponent(new ::logging::Component("MySQL", ::logging::writer::MySQL::Instantiate));
	plugin::Configuration config;
	config.name = "WingPig::MySQL";
	config.description = "Bro logging plugin, write log to MySQL.";
	config.version.major = 0;
	config.version.minor = 1;
	return config;
	}

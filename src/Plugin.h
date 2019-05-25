
#ifndef BRO_PLUGIN_WINGPIG_MYSQL
#define BRO_PLUGIN_WINGPIG_MYSQL

#include <plugin/Plugin.h>

namespace plugin {
namespace WingPig_MySQL {

class Plugin : public ::plugin::Plugin
{
protected:
	// Overridden from plugin::Plugin.
	plugin::Configuration Configure() override;
};

extern Plugin plugin;

}
}

#endif

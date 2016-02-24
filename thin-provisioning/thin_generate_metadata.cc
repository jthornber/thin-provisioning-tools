#include "thin-provisioning/commands.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

thin_generate_metadata_cmd::thin_generate_metadata_cmd()
	: command("thin_generate_metadata")
{
}

void
thin_generate_metadata_cmd::usage(std::ostream &out) const
{

}

int
thin_generate_metadata_cmd::run(int argc, char **argv)
{

	return 1;
}

//----------------------------------------------------------------

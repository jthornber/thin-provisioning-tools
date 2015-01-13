#include "device-mapper/interface.h"

#include <string.h>

using namespace dm;
using namespace std;

//----------------------------------------------------------------

void
version_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
remove_all_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
list_devices_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
create_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
remove_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
suspend_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
resume_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
clear_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
load_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
status_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
table_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
info_instr::execute(interface &dm)
{
	dm.execute(*this);
}

void
message_instr::execute(interface &dm)
{
	dm.execute(*this);
}

//----------------------------------------------------------------

void
interface::execute(instruction &instr)
{
	instr.execute(*this);
}

//----------------------------------------------------------------

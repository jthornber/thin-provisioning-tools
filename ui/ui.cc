#include "ui/ui.h"

#include <ncurses.h>

using namespace ui;

//----------------------------------------------------------------

text_ui::text_ui()
{
	initscr();
	noecho();

	start_color();
	init_pair(1, COLOR_RED, COLOR_BLACK);
	init_pair(2, COLOR_YELLOW, COLOR_BLACK);
	init_pair(3, COLOR_BLUE, COLOR_BLACK);
	init_pair(4, COLOR_GREEN, COLOR_BLACK);
	init_pair(5, COLOR_YELLOW, COLOR_BLACK);
	init_pair(6, COLOR_BLACK, COLOR_RED);
	init_pair(7, COLOR_WHITE, COLOR_BLACK);

}

text_ui::~text_ui()
{
	endwin();
}

void
text_ui::refresh()
{
	refresh();
}

//----------------------------------------------------------------

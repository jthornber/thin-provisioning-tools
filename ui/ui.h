#ifndef UI_UI_H

#include <ncurses.h>

//----------------------------------------------------------------

namespace ui {
	class text_ui {
	public:
		text_ui();
		~text_ui();

		void refresh();
	};
};


//----------------------------------------------------------------

#endif

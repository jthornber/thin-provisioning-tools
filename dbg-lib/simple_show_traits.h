#ifndef DBG_SIMPLE_SHOW_TRAITS_H
#define DBG_SIMPLE_SHOW_TRAITS_H

#include "dbg-lib/output_formatter.h"
#include "persistent-data/data-structures/simple_traits.h"

//----------------------------------------------------------------

namespace dbg {
	class uint32_show_traits {
	public:
		typedef persistent_data::uint32_traits value_trait;

		static void show(dbg::formatter::ptr f, std::string const &key, uint32_t const &value);
	};

	class uint64_show_traits {
	public:
		typedef persistent_data::uint64_traits value_trait;

		static void show(dbg::formatter::ptr f, std::string const &key, uint64_t const &value);
	};
}

//----------------------------------------------------------------

#endif

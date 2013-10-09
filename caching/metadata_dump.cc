#include "caching/metadata_dump.h"

using namespace std;
using namespace caching;

//----------------------------------------------------------------

namespace {
	string to_string(unsigned char const *data) {
		// FIXME: we're assuming the data is zero terminated here
		return std::string(reinterpret_cast<char const *>(data));
	}

	void raise_metadata_damage() {
		throw std::runtime_error("metadata contains errors (run cache_check for details).\n"
					 "perhaps you wanted to run with --repair");
	}

	//--------------------------------

	class mapping_emitter : public mapping_visitor {
	public:
		mapping_emitter(emitter::ptr e)
			: e_(e) {
		}

		void visit(block_address cblock, mapping const &m) {
			if (m.flags_ & M_VALID)
				e_->mapping(cblock, m.oblock_, m.flags_ & M_DIRTY);
		}

	private:
		emitter::ptr e_;
	};

	struct ignore_mapping_damage : public mapping_array_damage::damage_visitor {
		virtual void visit(mapping_array_damage::missing_mappings const &d) {}
		virtual void visit(mapping_array_damage::invalid_mapping const &d) {}
	};

	class fatal_mapping_damage : public mapping_array_damage::damage_visitor {
	public:
		virtual void visit(mapping_array_damage::missing_mappings const &d) {
			raise_metadata_damage();
		}

		virtual void visit(mapping_array_damage::invalid_mapping const &d) {
			raise_metadata_damage();
		}
	};

	//--------------------------------

	class hint_emitter : public hint_visitor {
	public:
		hint_emitter(emitter::ptr e)
			: e_(e) {
		}

		virtual void visit(block_address cblock, std::vector<unsigned char> const &data) {
			e_->hint(cblock, data);
		}

	private:
		emitter::ptr e_;
	};

	struct ignore_hint_damage : public hint_array_damage::damage_visitor {
		virtual void visit(hint_array_damage::missing_hints const &d) {}
	};

	class fatal_hint_damage : public hint_array_damage::damage_visitor {
		virtual void visit(hint_array_damage::missing_hints const &d) {
			raise_metadata_damage();
		}
	};
}

//----------------------------------------------------------------

void
caching::metadata_dump(metadata::ptr md, emitter::ptr e, bool repair)
{
	superblock const &sb = md->sb_;
	e->begin_superblock(to_string(sb.uuid), sb.data_block_size,
			    sb.cache_blocks, to_string(sb.policy_name),
			    sb.policy_hint_size);

	e->begin_mappings();
	{
		namespace mad = mapping_array_damage;

		mapping_emitter me(e);
		ignore_mapping_damage ignore;
		fatal_mapping_damage fatal;
		mad::damage_visitor &dv = repair ?
			static_cast<mad::damage_visitor &>(ignore) :
			static_cast<mad::damage_visitor &>(fatal);
		walk_mapping_array(*md->mappings_, me, dv);
	}
	e->end_mappings();

	// walk hints
	e->begin_hints();
	{
		using namespace hint_array_damage;

		hint_emitter he(e);
		ignore_hint_damage ignore;
		fatal_hint_damage fatal;
		damage_visitor &dv = repair ?
			static_cast<damage_visitor &>(ignore) :
			static_cast<damage_visitor &>(fatal);
		md->hints_->walk(he, dv);
	}
	e->end_hints();

	// FIXME: walk discards

	e->end_superblock();
}

//----------------------------------------------------------------


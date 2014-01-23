#include "era/metadata_dump.h"
#include "era/era_array.h"

using namespace era;
using namespace std;

//----------------------------------------------------------------

namespace {
	string to_string(unsigned char const *data) {
		// FIXME: we're assuming the data is zero terminated here
		return std::string(reinterpret_cast<char const *>(data));
	}

	void raise_metadata_damage() {
		throw std::runtime_error("metadata contains errors (run era_check for details).\n"
					 "perhaps you wanted to run with --repair ?");
	}

	class bloom_tree_emitter : public bloom_tree_detail::bloom_visitor {
	public:
		bloom_tree_emitter(emitter::ptr e)
			: e_(e) {
		}

		virtual void bloom_begin(uint32_t era, uint32_t nr_blocks, uint32_t nr_bits, uint32_t nr_set) {
			e_->begin_bloom(era, nr_bits, nr_blocks);
		}

		virtual void bit(uint32_t bit, bool value) {
			e_->bloom_bit(bit, value);
		}

		virtual void bloom_end() {
			e_->end_bloom();
		}

	private:
		emitter::ptr e_;
	};

	struct ignore_bloom_tree_damage : public bloom_tree_detail::damage_visitor {
		void visit(bloom_tree_detail::missing_eras const &d) {
		}

		void visit(bloom_tree_detail::damaged_bloom_filter const &d) {
		}
	};

	struct fatal_bloom_tree_damage : public bloom_tree_detail::damage_visitor {
		void visit(bloom_tree_detail::missing_eras const &d) {
			raise_metadata_damage();
		}

		void visit(bloom_tree_detail::damaged_bloom_filter const &d) {
			raise_metadata_damage();
		}
	};

	//--------------------------------

	class era_array_emitter : public era_array_visitor {
	public:
		era_array_emitter(emitter::ptr e)
			: e_(e) {
		}

		virtual void visit(uint32_t index, uint32_t era) {
			e_->era(index, era);
		}

	private:
		emitter::ptr e_;
	};

	struct ignore_era_array_damage : public era_array_detail::damage_visitor {
		void visit(era_array_detail::missing_eras const &d) {
		}

		void visit(era_array_detail::invalid_era const &d) {
		}
	};

	class fatal_era_array_damage : public era_array_detail::damage_visitor {
		void visit(era_array_detail::missing_eras const &d) {
			raise_metadata_damage();
		}

		void visit(era_array_detail::invalid_era const &d) {
			raise_metadata_damage();
		}
	};
}

//----------------------------------------------------------------

void
era::metadata_dump(metadata::ptr md, emitter::ptr e, bool repair)
{
	superblock const &sb = md->sb_;

	e->begin_superblock(to_string(sb.uuid), sb.data_block_size,
			    sb.nr_blocks,
			    sb.current_era);
	{
		{
			bloom_tree_emitter visitor(e);

			ignore_bloom_tree_damage ignore;
			fatal_bloom_tree_damage fatal;
			bloom_tree_detail::damage_visitor &dv = repair ?
				static_cast<bloom_tree_detail::damage_visitor &>(ignore) :
				static_cast<bloom_tree_detail::damage_visitor &>(fatal);

			walk_bloom_tree(md->tm_, *md->bloom_tree_, visitor, dv);
		}

		e->begin_era_array();
		{
			era_array_emitter visitor(e);

			ignore_era_array_damage ignore;
			fatal_era_array_damage fatal;
			era_array_detail::damage_visitor &dv = repair ?
				static_cast<era_array_detail::damage_visitor &>(ignore) :
				static_cast<era_array_detail::damage_visitor &>(fatal);

			walk_era_array(*md->era_array_, visitor, dv);
		}
		e->end_era_array();
	}
	e->end_superblock();
}

//----------------------------------------------------------------

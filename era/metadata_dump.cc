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

	class writeset_tree_emitter : public writeset_tree_detail::writeset_visitor {
	public:
		writeset_tree_emitter(emitter::ptr e)
			: e_(e) {
		}

		virtual void writeset_begin(uint32_t era, uint32_t nr_bits) {
			e_->begin_writeset(era, nr_bits);
		}

		virtual void bit(uint32_t bit, bool value) {
			e_->writeset_bit(bit, value);
		}

		virtual void writeset_end() {
			e_->end_writeset();
		}

	private:
		emitter::ptr e_;
	};

	class writeset_tree_collator : public writeset_tree_detail::writeset_visitor {
	public:
		writeset_tree_collator(map<uint32_t, uint32_t> &exceptions)
			: exceptions_(exceptions),
			  current_era_(0) {
		}

		virtual void writeset_begin(uint32_t era, uint32_t nr_bits) {
			current_era_ = era;
		}

		virtual void bit(uint32_t bit, bool value) {
			if (value) {
				map<uint32_t, uint32_t>::const_iterator it = exceptions_.find(bit);
				if (it == exceptions_.end() || it->second < current_era_)
					exceptions_.insert(make_pair(bit, current_era_));
			}
		}

		virtual void writeset_end() {
		}

	private:
		map<uint32_t, uint32_t> &exceptions_;
		uint32_t current_era_;
	};


	struct ignore_writeset_tree_damage : public writeset_tree_detail::damage_visitor {
		void visit(writeset_tree_detail::missing_eras const &d) {
		}

		void visit(writeset_tree_detail::damaged_writeset const &d) {
		}
	};

	struct fatal_writeset_tree_damage : public writeset_tree_detail::damage_visitor {
		void visit(writeset_tree_detail::missing_eras const &d) {
			raise_metadata_damage();
		}

		void visit(writeset_tree_detail::damaged_writeset const &d) {
			raise_metadata_damage();
		}
	};

	//--------------------------------

	class era_array_emitter : public era_array_visitor {
	public:
		era_array_emitter(emitter::ptr e, map<uint32_t, uint32_t> const &exceptions)
			: e_(e),
			  exceptions_(exceptions) {
		}

		virtual void visit(uint32_t index, uint32_t era) {
			map<uint32_t, uint32_t>::const_iterator it = exceptions_.find(index);
			if (it != exceptions_.end() && it->second > era)
				e_->era(index, it->second);
			else
				e_->era(index, era);
		}

	private:
		emitter::ptr e_;
		map<uint32_t, uint32_t> exceptions_;
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

	void
	dump(metadata::ptr md, emitter::ptr e, bool repair)
	{
		{
			writeset_tree_emitter visitor(e);

			ignore_writeset_tree_damage ignore;
			fatal_writeset_tree_damage fatal;
			writeset_tree_detail::damage_visitor &dv = repair ?
				static_cast<writeset_tree_detail::damage_visitor &>(ignore) :
				static_cast<writeset_tree_detail::damage_visitor &>(fatal);

			walk_writeset_tree(md->tm_, *md->writeset_tree_, visitor, dv);
		}

		e->begin_era_array();
		{
			map<uint32_t, uint32_t> exceptions;
			era_array_emitter visitor(e, exceptions);

			ignore_era_array_damage ignore;
			fatal_era_array_damage fatal;
			era_array_detail::damage_visitor &dv = repair ?
				static_cast<era_array_detail::damage_visitor &>(ignore) :
				static_cast<era_array_detail::damage_visitor &>(fatal);

			walk_era_array(*md->era_array_, visitor, dv);
		}
		e->end_era_array();
	}

	void dump_logical(metadata::ptr md, emitter::ptr e, bool repair)
	{
		// This will potentially use a lot of memory, but I don't
		// see a way around it.
		map<uint32_t, uint32_t> exceptions;

		{
			writeset_tree_collator visitor(exceptions);

			ignore_writeset_tree_damage ignore;
			fatal_writeset_tree_damage fatal;
			writeset_tree_detail::damage_visitor &dv = repair ?
				static_cast<writeset_tree_detail::damage_visitor &>(ignore) :
				static_cast<writeset_tree_detail::damage_visitor &>(fatal);

			walk_writeset_tree(md->tm_, *md->writeset_tree_, visitor, dv);
		}

		e->begin_era_array();
		{
			era_array_emitter visitor(e, exceptions);

			ignore_era_array_damage ignore;
			fatal_era_array_damage fatal;
			era_array_detail::damage_visitor &dv = repair ?
				static_cast<era_array_detail::damage_visitor &>(ignore) :
				static_cast<era_array_detail::damage_visitor &>(fatal);

			walk_era_array(*md->era_array_, visitor, dv);
		}
		e->end_era_array();
	}
}

//----------------------------------------------------------------

void
era::metadata_dump(metadata::ptr md, emitter::ptr e,
		   bool repair, bool logical)
{
	superblock const &sb = md->sb_;
	e->begin_superblock(to_string(sb.uuid), sb.data_block_size,
			    sb.nr_blocks,
			    sb.current_era);
	{
		if (logical)
			dump_logical(md, e, repair);
		else
			dump(md, e, repair);
	}
	e->end_superblock();
}

//----------------------------------------------------------------

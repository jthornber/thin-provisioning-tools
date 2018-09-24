// Copyright (C) 2018 Red Hat, Inc. All rights reserved.
//
// This file is part of the thin-provisioning-tools source.
//
// thin-provisioning-tools is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.
//
// thin-provisioning-tools is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with thin-provisioning-tools.  If not, see
// <http://www.gnu.org/licenses/>.

#ifndef THIN_PROVISIONING_THIN_JOURNAL_H
#define THIN_PROVISIONING_THIN_JOURNAL_H

#include "persistent-data/block.h"

#include <vector>

//----------------------------------------------------------------

namespace thin_provisioning {
	uint32_t const JOURNAL_BLOCK_SIZE = 256 * 1024;
	uint32_t const JOURNAL_CHUNK_SIZE = 32;
	uint32_t const JOURNAL_NR_CHUNKS = (4096 / JOURNAL_CHUNK_SIZE);

	class byte_stream {
	public:
		byte_stream(persistent_data::block_manager<JOURNAL_BLOCK_SIZE>::ptr bm);

		void read_bytes(uint8_t *b, uint8_t *e);

	private:
		void next_block_();
		size_t read_some_(uint8_t *b, uint8_t *e);

		persistent_data::block_manager<JOURNAL_BLOCK_SIZE>::ptr bm_;

		uint64_t current_block_;
		uint64_t cursor_;
	};

	//---------------------------------

	class journal_visitor;

	struct journal_msg {
		journal_msg(bool success);

		virtual ~journal_msg() {}
		virtual void visit(journal_visitor &v) const = 0;

		bool success_;
	};

	struct open_journal_msg : public journal_msg {
		open_journal_msg(uint64_t nr_metadata_blocks);
		virtual void visit(journal_visitor &v) const;
		uint64_t nr_metadata_blocks_;
	};

	struct close_journal_msg : public journal_msg {
		close_journal_msg();
		virtual void visit(journal_visitor &v) const;
	};

	struct block_msg : public journal_msg {
		block_msg(bool success, uint64_t index);
		uint64_t index_;
	};

	struct read_lock_msg : public block_msg {
		read_lock_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct write_lock_msg : public block_msg {
		write_lock_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct zero_lock_msg : public block_msg {
		zero_lock_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct try_read_lock_msg : public block_msg {
		try_read_lock_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct delta {
		delta(uint32_t offset, std::vector<uint8_t> &bytes)
			: offset_(offset),
			  bytes_(bytes) {
		}

		uint32_t offset_;
		std::vector<uint8_t> bytes_;
	};

	using delta_list = std::vector<delta>;

	struct unlock_msg : public block_msg {
		unlock_msg(bool success, uint64_t index, delta_list const &deltas);
		virtual void visit(journal_visitor &v) const;

		delta_list deltas_;
	};

	struct verify_msg : public block_msg {
		verify_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct prepare_msg : public block_msg {
		prepare_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct flush_msg : public journal_msg {
		flush_msg(bool success);
		virtual void visit(journal_visitor &v) const;
	};

	struct flush_and_unlock_msg : public block_msg {
		flush_and_unlock_msg(bool success, uint64_t index, delta_list const &deltas);
		virtual void visit(journal_visitor &v) const;

		delta_list deltas_;
	};

	struct prefetch_msg : public block_msg {
		prefetch_msg(bool success, uint64_t index);
		virtual void visit(journal_visitor &v) const;
	};

	struct set_read_only_msg : public journal_msg {
		set_read_only_msg();
		virtual void visit(journal_visitor &v) const;
	};

	struct set_read_write_msg : public journal_msg {
		set_read_write_msg();
		virtual void visit(journal_visitor &v) const;
	};

	struct journal_visitor {
	public:
		virtual ~journal_visitor() {};

		void visit(journal_msg const &msg) {
			msg.visit(*this);
		}

		virtual void visit(open_journal_msg const &msg) = 0;
		virtual void visit(close_journal_msg const &msg) = 0;
		virtual void visit(read_lock_msg const &msg) = 0;
		virtual void visit(write_lock_msg const &msg) = 0;
		virtual void visit(zero_lock_msg const &msg) = 0;
		virtual void visit(try_read_lock_msg const &msg) = 0;
		virtual void visit(unlock_msg const &msg) = 0;
		virtual void visit(verify_msg const &msg) = 0;
		virtual void visit(prepare_msg const &msg) = 0;
		virtual void visit(flush_msg const &msg) = 0;
		virtual void visit(flush_and_unlock_msg const &msg) = 0;
		virtual void visit(prefetch_msg const &msg) = 0;
		virtual void visit(set_read_only_msg const &msg) = 0;
		virtual void visit(set_read_write_msg const &msg) = 0;
	};

	enum msg_type {
		MT_OPEN_JOURNAL,
		MT_CLOSE_JOURNAL,

		MT_READ_LOCK,
		MT_WRITE_LOCK,
		MT_ZERO_LOCK,
		MT_TRY_READ_LOCK,
		MT_UNLOCK,
		MT_VERIFY,
		MT_PREPARE,
		MT_FLUSH,
		MT_FLUSH_AND_UNLOCK,
		MT_PREFETCH,
		MT_SET_READ_ONLY,
		MT_SET_READ_WRITE,
	};

	class journal {
	public:
		journal(persistent_data::block_manager<JOURNAL_BLOCK_SIZE>::ptr bm);
		void read_journal(struct journal_visitor &v);

	private:
		bool read_delta_(delta_list &ds);
		delta_list read_deltas_();
		bool read_one_(struct journal_visitor &v);

		template <typename T> T read_() {
			T r;
			in_.read_bytes(reinterpret_cast<uint8_t *>(&r), reinterpret_cast<uint8_t *>(&r + 1));
			return r;
		}

		byte_stream in_;
	};
}

//----------------------------------------------------------------

#endif

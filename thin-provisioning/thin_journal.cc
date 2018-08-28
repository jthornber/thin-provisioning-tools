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

#include "thin-provisioning/thin_journal.h"

#include <algorithm>

using namespace thin_provisioning;
using namespace persistent_data;
using namespace std;

//----------------------------------------------------------------

byte_stream::byte_stream(block_manager<JOURNAL_BLOCK_SIZE>::ptr bm)
		: bm_(bm),
		  current_block_(0),
		  cursor_(0)
{
}

void
byte_stream::read_bytes(uint8_t *b, uint8_t *e)
{
	while (b != e)
		b += read_some_(b, e);
}

void
byte_stream::next_block_()
{
	current_block_++;
}

size_t
byte_stream::read_some_(uint8_t *b, uint8_t *e)
{
	if (cursor_ == JOURNAL_BLOCK_SIZE)
		next_block_();

	size_t len = min<uint64_t>(e - b, JOURNAL_BLOCK_SIZE - cursor_);
	auto rr = bm_->read_lock(current_block_);

	uint8_t const *data_begin = reinterpret_cast<uint8_t const *>(rr.data()) + cursor_;
	memcpy(b, data_begin, len);
	cursor_ += len;

	return len;
}

//---------------------------------

journal_msg::journal_msg(bool success)
	: success_(success)
{
}

block_msg::block_msg(bool success, uint64_t index) 
	: journal_msg(success), index_(index)
{
}

read_lock_msg::read_lock_msg(bool success, uint64_t index) 
	: block_msg(success, index)
{
}

void
read_lock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

write_lock_msg::write_lock_msg(bool success, uint64_t index) 
	: block_msg(success, index)
{
}

void
write_lock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

zero_lock_msg::zero_lock_msg(bool success, uint64_t index) 
	: block_msg(success, index)
{
}

void
zero_lock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

try_read_lock_msg::try_read_lock_msg(bool success, uint64_t index) 
	: block_msg(success, index)
{
}

void
try_read_lock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

unlock_msg::unlock_msg(bool success, uint64_t index, delta_list const &deltas)
	: block_msg(success, index),
	  deltas_(deltas)
{
}

void
unlock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

verify_msg::verify_msg(bool success, uint64_t index) 
	: block_msg(success, index)
{
}

void
verify_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

prepare_msg::prepare_msg(bool success, uint64_t index)
	: block_msg(success, index)
{
}

void
prepare_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

flush_msg::flush_msg(bool success)
	: journal_msg(success)
{
}

void
flush_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

flush_and_unlock_msg::flush_and_unlock_msg(bool success, uint64_t index, delta_list const &deltas)
	: block_msg(success, index),
	  deltas_(deltas)
{
}

void
flush_and_unlock_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

prefetch_msg::prefetch_msg(bool success, uint64_t index)
	: block_msg(success, index)
{
}

void
prefetch_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

set_read_only_msg::set_read_only_msg()
	: journal_msg(true)
{
}

void
set_read_only_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

set_read_write_msg::set_read_write_msg()
	: journal_msg(true)
{
}

void
set_read_write_msg::visit(journal_visitor &v) const
{
	v.visit(*this);
}

//------------------------------------------

journal::journal(block_manager<JOURNAL_BLOCK_SIZE>::ptr bm)
	: in_(bm)
{
}

void
journal::read_journal(struct journal_visitor &v)
{
	while (read_one_(v))
		;
}

bool
journal::read_one_(struct journal_visitor &v)
{
	uint8_t header = read_<uint8_t>();
	uint8_t t = header >> 1;
	uint8_t success = header & 0x1;
	uint64_t index;

	switch (static_cast<msg_type>(t)) {
	case MT_READ_LOCK:
		index = read_<uint64_t>();
		v.visit(read_lock_msg(success, index));
		break;

	case MT_WRITE_LOCK:
		index = read_<uint64_t>();
		v.visit(write_lock_msg(success, index));
		break;

	case MT_ZERO_LOCK:
		index = read_<uint64_t>();
		v.visit(zero_lock_msg(success, index));
		break;

	case MT_TRY_READ_LOCK:
		index = read_<uint64_t>();
		v.visit(try_read_lock_msg(success, index));
		break;

	case MT_UNLOCK: {
		index = read_<uint64_t>();
		auto deltas = read_deltas_();
		v.visit(unlock_msg(success, index, deltas));
	}
		break;

	case MT_VERIFY:
		index = read_<uint64_t>();
                v.visit(verify_msg(success, index));
		break;

	case MT_PREPARE:
                index = read_<uint64_t>();
                v.visit(prepare_msg(success, index));
		break;

	case MT_FLUSH:
                v.visit(flush_msg(success));
		break;

	case MT_FLUSH_AND_UNLOCK: {
		index = read_<uint64_t>();
		auto deltas = read_deltas_();
                v.visit(flush_and_unlock_msg(success, index, deltas));
		}
		break;

	case MT_PREFETCH:
		index = read_<uint64_t>();
                v.visit(prefetch_msg(success, index));
		break;

	case MT_SET_READ_ONLY:
                v.visit(set_read_only_msg());
		break;

	case MT_SET_READ_WRITE:
                v.visit(set_read_write_msg());
		break;

	case MT_END_OF_JOURNAL:
		return false;
	}

	return true;
}

bool
journal::read_delta_(delta_list &ds)
{
	uint8_t chunk = read_<uint8_t>();

	if (chunk == 0xff)
		return false;

	auto bytes = vector<uint8_t>(JOURNAL_CHUNK_SIZE, 0);
	in_.read_bytes(bytes.data(), bytes.data() + JOURNAL_CHUNK_SIZE);
	ds.push_back(delta(chunk, bytes));

	return true;
}

thin_provisioning::delta_list
journal::read_deltas_()
{
	delta_list ds;

	while (read_delta_(ds))
		;

	return ds;
}

//----------------------------------------------------------------


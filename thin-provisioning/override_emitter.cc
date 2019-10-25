// Copyright (C) 2019 Red Hat, Inc. All rights reserved.
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

#include "thin-provisioning/override_emitter.h"

using namespace thin_provisioning;

//----------------------------------------------------------------

namespace {
        class override_emitter : public emitter {
        public:
                override_emitter(emitter::ptr inner, override_options const &opts)
                        : inner_(inner),
                          opts_(opts) {
                }

                virtual void begin_superblock(std::string const &uuid,
                                              uint64_t time,
                                              uint64_t trans_id,
                                              boost::optional<uint32_t> flags,
                                              boost::optional<uint32_t> version,
                                              uint32_t data_block_size,
                                              uint64_t nr_data_blocks,
                                              boost::optional<uint64_t> metadata_snap) {
                        inner_->begin_superblock(uuid, time, opts_.get_transaction_id(trans_id),
                                                 flags, version, opts_.get_data_block_size(data_block_size),
                                                 opts_.get_nr_data_blocks(nr_data_blocks),
                                                 metadata_snap);
                }

                virtual void end_superblock() {
                        inner_->end_superblock();
                }

                virtual void begin_device(uint32_t dev,
                                          uint64_t mapped_blocks,
                                          uint64_t trans_id,
                                          uint64_t creation_time,
                                          uint64_t snap_time) {
                        inner_->begin_device(dev, mapped_blocks, trans_id, creation_time, snap_time);
                }

                virtual void end_device() {
                        inner_->end_device();
                }

                virtual void begin_named_mapping(std::string const &name) {
                        inner_->begin_named_mapping(name);
                }

                virtual void end_named_mapping() {
                        inner_->end_named_mapping();
                }

                virtual void identifier(std::string const &name) {
                        inner_->identifier(name);
                }

                virtual void range_map(uint64_t origin_begin, uint64_t data_begin, uint32_t time, uint64_t len) {
                        inner_->range_map(origin_begin, data_begin, time, len);
                }

                virtual void single_map(uint64_t origin_block, uint64_t data_block, uint32_t time) {
                        inner_->single_map(origin_block, data_block, time);
                }

        private:
                emitter::ptr inner_;
                override_options opts_;
        };
}

emitter::ptr thin_provisioning::create_override_emitter(emitter::ptr inner, override_options const &opts)
{
        return emitter::ptr(new override_emitter(inner, opts));
}

//----------------------------------------------------------------


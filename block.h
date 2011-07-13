#ifndef BLOCK_H
#define BLOCK_H

#include <stdint.h>
#include <map>
#include <vector>

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <boost/shared_ptr.hpp>

//----------------------------------------------------------------

namespace persistent_data {

	typedef uint64_t block_address;

	template <uint32_t BlockSize>
	class block_manager : private boost::noncopyable {
	public:
		typedef boost::shared_ptr<block_manager> ptr;

		block_manager(std::string const &path, block_address nr_blocks);
		~block_manager();

		typedef unsigned char buffer[BlockSize];
		typedef unsigned char const const_buffer[BlockSize];

		class block;

		class validator {
		public:
			typedef boost::shared_ptr<validator> ptr;

			virtual ~validator() {}

			virtual void check(block const &b) const = 0;
			virtual void prepare(block &b) const = 0;
		};

		struct block {
			typedef boost::shared_ptr<block> ptr;
			typedef boost::optional<typename validator::ptr> maybe_validator;

			block(block_address location,
			      maybe_validator v = maybe_validator())
				: location_(location),
				  validator_(v),
				  initialised_(false) {
			}

			block_address location_;
			buffer data_;
			maybe_validator validator_;
			bool initialised_;
		};

		class read_ref {
		public:
			read_ref(typename block::ptr b);
			virtual ~read_ref() {}

			block_address get_location() const;
			const_buffer &data() const;

		protected:
			typename block::ptr block_;
		};

		// Inherited from read_ref, since you can read a block that's write
		// locked.
		class write_ref : public read_ref {
		public:
			write_ref(typename block::ptr b);

			using read_ref::data;
			buffer &data();
		};

		// Locking methods
		read_ref
		read_lock(block_address location) const;

		boost::optional<read_ref>
		read_try_lock(block_address location) const;

		write_ref
		write_lock(block_address location);

		write_ref
		write_lock_zero(block_address location);

		// Validator variants
		read_ref
		read_lock(block_address location,
			  typename validator::ptr const &v) const;

		boost::optional<read_ref>
		read_try_lock(block_address location,
			      typename validator::ptr const &v) const;

		write_ref
		write_lock(block_address location,
			   typename validator::ptr const &v);

		write_ref
		write_lock_zero(block_address location,
				typename validator::ptr const &v);

		// Use this to commit changes
		void flush(write_ref super_block);

	private:
		void check(block_address b) const;

		void read_block(block &b) const;
		void write_block(block const &b);
		void zero_block(block &b);
		void write_and_release(block *b);

		int fd_;
		block_address nr_blocks_;
	};
}

#include "block.tcc"

//----------------------------------------------------------------

#endif

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

		block_manager(std::string const &path);
		~block_manager();

		typedef unsigned char buffer[BlockSize];
		typedef unsigned char const const_buffer[BlockSize];

		class block;

		class block_validator {
		public:
			virtual ~block_validator() {}

			virtual void check(block const &b) const = 0;
			virtual void prepare(block &b) const = 0;
		};

		struct block {
			typedef boost::optional<block_validator> maybe_validator;

			block(block_address location,
			      maybe_validator v = maybe_validator())
				: location_(location),
				  validator_(v) {
			}

			block_address location_;
			buffer data_;
			boost::optional<block_validator> validator_;
		};
		typedef boost::shared_ptr<block> block_ptr;

		class read_ref {
		public:
			read_ref(block_ptr b);
			virtual ~read_ref() {}

			block_address get_location() const;
			const_buffer &data() const;

		protected:
			block_ptr block_;
		};

		// Inherited from read_ref, since you can read a block that's write
		// locked.
		class write_ref : public read_ref {
		public:
			using read_ref::data;
			buffer &data();
		};

		// Locking methods
		read_ref
		read_lock(block_address location);

		boost::optional<read_ref>
		read_try_lock(block_address location);

		write_ref
		write_lock(block_address location);

		write_ref
		write_lock_zero(block_address location);

		// Validator variants
		read_ref
		read_lock(block_address location, block_validator const &v);

		boost::optional<read_ref>
		read_try_lock(block_address location, block_validator const &v);

		write_ref
		write_lock(block_address location, block_validator const &v);

		write_ref
		write_lock_zero(block_address location, block_validator const &v);

		// Use this to commit changes
		void flush(write_ref super_block);

	private:
		void read_block(block &b);
		void write_block(block const &b);
		void zero_block(block &b);

		void write_and_release(block *b);

		int fd_;
	};
}

//----------------------------------------------------------------

#endif

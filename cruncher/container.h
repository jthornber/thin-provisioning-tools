#ifndef CRUNCHER_CONTAINER_H
#define CRUNCHER_CONTAINER_H

//----------------------------------------------------------------

namespace cruncher {
	class chunk_hash {

	};

	// Containers just store fragments, and provide an index.  No
	// concept of a 'stream'. The decision to compress is up to the
	// container.
	class container {
	public:
		typedef boost::shared_ptr<container> ptr;

		virtual ~container() {}

		virtual void add_chunk(chunk_hash const &id, chunk const &c) = 0;
		virtual void get_chunk(chunk_hash const &id, chunk &c) const = 0;

		virtual bool contains_chunk(chunk_hash const &id) const = 0;
	};

	container::ptr create_null_container
}

//----------------------------------------------------------------

#endif

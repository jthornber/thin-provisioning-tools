#ifndef CRUNCHER_CONTAINER_MANAGER_H
#define CRUNCHER_CONTAINER_MANAGER_H

//----------------------------------------------------------------

namespace cruncher {
	//
	class chunk_hash {

	};

	// Containers just store fragments, and provide an index.  No
	// concept of a 'stream'.
	class container {
	public:
		typedef boost::shared_ptr<container> ptr;

		virtual ~container() {}

		virtual void add_chunk(chunk_hash const &id, chunk const &c) = 0;
		virtual void close() = 0;
	};

	class output_stream {
	};

	class input_stream {
		virtual uint64_t size() = 0;
		virtual chunk const &next_chunk();
	};

	class container_manager {
	public:
		void open_stream(uuid const &id);


	};
}

//----------------------------------------------------------------

#endif

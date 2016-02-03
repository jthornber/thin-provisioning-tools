#ifndef CRUNCHER_STREAM_COMMANDS_H
#define CRUNCHER_STREAM_COMMANDS_H

//----------------------------------------------------------------

namespace cruncher {
	class address_space {

	};

	class chunk;

	class stream {
	public:
		chunk &new_chunk();
		void append(chunk &chunk);
	};

	class stream_manager {
	public:
		stream_manager(address_space &sp);

		typedef uint64_t stream_id;

		stream_id new_stream();


	private:
	};
}

//----------------------------------------------------------------

#endif 

# Introduction

This document describes a suite of software components that I'm calling
'cruncher' for now.  Initially these will be targeted towards thin provisioning
users, but I think it's applicable to lots of different backup scenarios.

# Components

There are 3 categories of components.  Possibly with multiple implementations of
each.

## Container

A container stores multiple streams of data.  New streams can be added at any
time.  Ranges of data from the streams can be retrieved (typically all of it).
Unlike a block device, random access performance is expected to be poor.

The initial proof of concept implementation will probably just use large files
to store the streams.  But the main one I'm working on uses data deduplication
and compression to reduce the storage requirements.  Deduplication is performed
between all streams, so incremental backups will hopefully be very compressed.

The container does not know what the data is.  It stores no metadata about
the data stream, other than a UUID.

## Transport

Communication between a front end and a container will be done via a transport
library.  The main (possibly only) implementation of this will use [zero
mq](http://zeromq.org/) in various configurations to allow us to scale from
small, in process containers to cloud based distributed containers.

The transport will support compression, but this will be performance based, and
in no way dictate how a container compresses or stores the streams.

## Front end

Front ends build and retrieve streams.

The front end is responsible for splitting the stream up into small (eg, 4k)
chunks.  To maximise the effectiveness of data deduplication the chunk will need
to be content based (ie. use a rolling hash).  See [thin_show_duplicates][1] for
an example.

Each chunk of the stream can be of the following type:

- All zeroes
- Marked as a duplicate of a range in another stream.  This lets us take advantage
  of known duplication such as sharing between thinp snapshots.
- Real data

If the chunk contains real data, then an SHA1 hash is calculated for it and sent
to the container.  The container will asynchronously either request the data for
that chunk, or tell the front end that it already has it (via dedup).

[1]: https://github.com/jthornber/thin-provisioning-tools/blob/2015-08-19-thin-show-duplicates/thin-provisioning/variable_chunk_stream.h


### Front end metadata

Each front end will need to store it's own metadata.  For example tying
particular stream uuids to thin snapshots, keeping a record of the hierarchy of
snapshots, recording which files need to be backed up etc.  The sensible place
to store this metadata is as a stream, within a container.  This way the front
end should just need to be configured with connection details for the container,
and a stream UUID for its metadata.

### Example front ends

- **thin_archive**:  Part of the thin-provisioning-tools package.  This allows the
  admin to archive a snapshot to a long term container store.  After this the
  snapshot can be deleted.  If it's needed again then the snap can be restored via
  thin_archive.  This takes a lot of pressure off the thin pool, and I hope
  becomes integrated in everyone's thinp best practice.

- **file backup**:  This front end keeps track of directories and files that
  have been flagged for backup and periodically transfers then to a container.

- **Replication**:  By archiving to a container, then restoring to a different
  machine we can implement remote replication.


# Misc thoughts

## Can we encrypt the data in the containers?

Data centers like this, since it adds security to client data, and data can be
effectively deleted by 'forgetting' the keys.  There are three scenarios I can
see here:

1. Encrypt the device the containers are stored on.  All streams end up
encrypted with the same key.  Dedup still works well.  Front ends need know
nothing about the encryption.

   I think most people concerned about encryption want finer grain control.  eg,
   encrypting the data for different clients with separate keys.  We could
   provide this by having one container per client.  But that loses the benefits
   of dedup with VM images.

2. Front end encrypts before splitting into chunks.  The SHA1 hashes will now be
of encrypted data so we risk losing the benefits of deduplication; if the front
end used a different key for each client then deduplication would only occur
within a clients data.  A big loss when backing up VM images.  Prior encryption
like this will also drastically reduce the effectiveness of compression.
Container doesn't need to know anything about encryption.

3. Some complicated middle ground where the container creates and manages keys
and groups streams within a particular key.  Unencrypted data is passed to the
container.  SHA1 hashes are based on unencrypted data.  Duplicate references are
only allowed within a client.  Compression would not be defeated since the
container could compress before encrypting.

## Replication

Some front ends could double as containers for replication

For instance thin_archive could send a stream to another instance of
thin_archive which would be restoring it to another pool.

## Restricting access to streams

Do we need permissions?  eg, 'these clients/front ends can read this stream',
'these clients/front ends are allowed to create streams'.  How does a client
prove who it is?  Is this related to encryption (an unauthorised front end won't
know the key a stream was encrypted with).

## Universal stream reference

A nice URL like way of referencing a stream.  Should containers publish an index
of their contents?

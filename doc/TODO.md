# TODO Items

Items in the High or Medium sections should be done before the formal release. The rest of the items could be fixes in further releases.

## High

- [ ] app: Ensure command line interface compatibilities,
      (e.g., the quiet flag, LVM compatibility, etc.)
- [x] thin_dump --skip-mappings
- [x] thin_dump: Display hints on input error
- [ ] Handle unexpected input values (zero or negative) in some program,
      e.g., `cache_metadata_size --block-size 0` causes floating point exception.

## Medium

- [x] thin_shrink: Support shared mappings
- [ ] cache_writeback: Replace io_uring by sync & threaded IO
- [ ] cache_writeback: Support large chunk size
- [ ] thin_shrink: Migrate to the sync copier
- [ ] btree_builder: Check pushed key ordering
- [ ] thin/cache_metadata_size: Improve output precision (for Stratis)
- [x] cache_restore: Support v1 metadata

## Performance Issues

- [x] thin_repair: Reduce time complexity of find_roots
- [ ] thin_repair: Multi-threaded find_roots
- [ ] thin_dump: The iops seems lower than the C++ version
- [ ] thin_delta: Multi-threaded get_mappings()
- [ ] cache_check: Multi-threaded checking
- [ ] io_engine: Remove locks from SyncIoEngine

## Low or Enhancements

- [ ] thin_dump: Simplify the algorithms for Gatherer (it's a subgraph searching problem in an acyclic directed graph)
- [ ] thin_dump: Detect cycles during metadata optimization
- [ ] thin_restore/repair: Recount the number of mappings in device details
- [ ] Progress bar for thin_repair/thin_dump/cache_writeback/cache_check/... etc
- [ ] thin_restore: Reduce the number of bitmap updates in write_metadata_sm(): read & write each bitmap block only once
      (Not very important since the allocation ranges are not fragmented typically)
- [ ] all: Return meaningful errno to the main function (refer to the error code returned by dm-thin, e.g., NOSPC while metadata is full)
- [ ] thin_explore: Handle broken nodes
- [ ] thin_explore: Dump space maps
- [ ] thin_explore: Improve usability (e.g., support pgup/pgdown browsing)
- [ ] thin/cache/era_dump: Show output errors except the broken pipe error
- [ ] thin/cache/era_repair: Clear superblock if the output is incompleted (commit 1dd7b454, bz1499781)
      (Is it really necessary? In addition, issuing IO in error handling routine seems not a good idea)

## RFEs

- [x] thin_shrink: Support binary-to-binary translation
- [ ] thin_repair: Rebuild the device details tree even though it's unavailable

## Tests

- [ ] tests: Use packed metadata for all the programs incl. cache/era tools, to reduce coupling between tests
- [ ] tests: Do not remove the test directory if an external program failed
- [ ] thin_generate_fmetadata: Finish the metadata generator
- [ ] btree_walker: Ensure visited nodes are counted (btree_walker with sm)
- [ ] btree_builder: Verify ref counts of written nodes
- [ ] cache_generate_damage to write an invalid superblock version (replaces `cache_restore --debug-override-metadata-version`)
- [ ] cache_explore, era_explore

## Cleanups

- [ ] Remove unnecessary Arc & Mutex in WriteBatcher, etc., if thin_restore/repair is surely a single threaded program
- [ ] metadata.rs: Do we really need to track KeyRange while optimizing metadata?
      (Is that for cosmetic purpose, e.g., for thin_dump to print the key ranges of the def/ref tags?)
- [ ] Error handling in command line parsing: Returns recoverable Error instead of exit() directly
      (Functions that invokes exit(): value_of_t_or_exit(), check_input_file(), ...)
- [ ] Replace anyhow::Result by std::io::Result in low-level functions (string parsing, space map operations, etc.)
- [ ] Make the error messages in utility functions more descriptive (not just a short "stat failed")
- [ ] Merge the bitset iteration functions
- [x] thin_check: Factor out functions that uses IoEngine from `src/commands`
- [ ] Clean up assert & panics
- [ ] thin_dump: Fix MappingVisitor::visit_again(): should we invoke ref_shared() in this function?
- [ ] Should we invoke Visitor::end_walk() on every node?
- [ ] Preserve the error payload in array/btree Errors (maybe a Boxed Error like std::io::Error::other() or anyhow::Error::from())
- [x] thin_dump: Optional argument for --metadata-snap
- [ ] Remove blank lines in help (clap.git issue #2983)


use super::*;

use tempfile::{NamedTempFile, TempPath};

use crate::mempool::MemPool;

//-----------------------------------------

const BLOCK_SIZE: u32 = 32768; // 32 KB
const QUEUE_DEPTH: u32 = 64;
const FILE_SIZE: u32 = 33554432; // 32 MB

struct AioEngineTests {
    pool: MemPool,
    paths: [TempPath; 2],
    engine: AioEngine,
}

impl AioEngineTests {
    fn new(block_size: u32, queue_depth: u32, file_size: u32) -> io::Result<Self> {
        let file0 = NamedTempFile::new_in("./")?;
        unsafe {
            libc::fallocate(file0.as_raw_fd(), 0, 0, file_size as libc::off_t);
        }

        let file1 = NamedTempFile::new_in("./")?;
        unsafe {
            libc::fallocate(file1.as_raw_fd(), 0, 0, file_size as libc::off_t);
        }

        Ok(AioEngineTests {
            pool: MemPool::new(block_size as usize, queue_depth as usize * 2)?,
            paths: [file0.into_temp_path(), file1.into_temp_path()],
            engine: AioEngine::new(queue_depth)?,
        })
    }
}

#[test]
fn open_and_close_multiple_handles() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let src_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), false, true)
        .expect("cannot open the source file");
    let dest_handle = fixture
        .engine
        .open_file(fixture.paths[1].as_ref(), true, true)
        .expect("cannot open the dest file");
    assert!(fixture.engine.close_file(src_handle).is_ok());
    assert!(fixture.engine.close_file(dest_handle).is_ok());
}

#[test]
fn read_a_read_only_handle_should_succeed() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let src_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), false, true)
        .expect("cannot open the source file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(src_handle, AioOp::Read, 0, BLOCK_SIZE, buf.data, context)
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Ok(c)) if c == &context));

    assert!(fixture.engine.close_file(src_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

#[test]
fn write_to_a_read_only_handle_should_fail() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let dest_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), false, true)
        .expect("cannot open the dest file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(dest_handle, AioOp::Write, 0, BLOCK_SIZE, buf.data, context)
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Err((c, _))) if c == &context));

    assert!(fixture.engine.close_file(dest_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

#[test]
fn write_to_a_read_write_handle_should_succeed() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let dest_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), true, true)
        .expect("cannot open the source file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(dest_handle, AioOp::Write, 0, BLOCK_SIZE, buf.data, context)
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Ok(c)) if c == &context));

    assert!(fixture.engine.close_file(dest_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

#[test]
fn read_the_last_block_should_succeed() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let src_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), false, true)
        .expect("cannot open the source file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(
            src_handle,
            AioOp::Read,
            (FILE_SIZE - BLOCK_SIZE) as libc::off_t,
            BLOCK_SIZE,
            buf.data,
            context
        )
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Ok(c)) if c == &context));

    assert!(fixture.engine.close_file(src_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

#[test]
fn out_of_bounds_read_fails() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let src_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), false, true)
        .expect("cannot open the source file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(
            src_handle,
            AioOp::Read,
            FILE_SIZE as libc::off_t,
            BLOCK_SIZE,
            buf.data,
            context
        )
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Err((c, _))) if c == &context));

    assert!(fixture.engine.close_file(src_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

#[test]
fn out_of_bounds_write_succeeds() {
    let mut fixture = AioEngineTests::new(BLOCK_SIZE, QUEUE_DEPTH, FILE_SIZE)
        .expect("cannot create test fixtures");
    let dest_handle = fixture
        .engine
        .open_file(fixture.paths[0].as_ref(), true, true)
        .expect("cannot open the dest file");

    let buf = fixture.pool.alloc().expect("cannot alloc a free buffer");
    let context = 123;
    assert!(fixture
        .engine
        .issue(
            dest_handle,
            AioOp::Write,
            FILE_SIZE as libc::off_t,
            BLOCK_SIZE,
            buf.data,
            context
        )
        .is_ok());

    let res = fixture.engine.wait();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res.first(), Some(Ok(c)) if c == &context));

    assert!(fixture.engine.close_file(dest_handle).is_ok());
    fixture.pool.free(buf).expect("pool free");
}

//-----------------------------------------

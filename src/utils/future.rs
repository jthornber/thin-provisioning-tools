use std::sync::mpsc;
use std::thread;

//--------------------------------

/// Spawns a computation in a new thread and returns a closure to retrieve its result.
///
/// This function allows for concurrent execution of a task without using async/await syntax.
/// It immediately returns a closure that, when called, will block until the spawned task completes
/// and then return its result.
///
/// # Arguments
///
/// * `work` - A closure that represents the work to be done in the spawned thread.
///   This closure must be `Send` and `'static`.
///
/// # Returns
///
/// Returns a closure that, when called, blocks and returns the result of the spawned work.
/// The returned closure is `FnOnce` because it can only be called once.
///
/// # Type Parameters
///
/// * `F` - The type of the work closure. Must implement `FnOnce() -> T`.
/// * `T` - The return type of the work closure. Must be `Send` and `'static`.
///
/// # Examples
///
/// ```
/// use thinp::utils::future::spawn_future;
///
/// let future = spawn_future(|| {
///     // Some expensive computation
///     std::thread::sleep(std::time::Duration::from_secs(2));
///     42
/// });
///
/// // Do other work here
///
/// let result = future(); // Blocks until the result is ready
/// assert_eq!(result, 42);
/// ```
///
/// # Panics
///
/// This function will panic if the spawned thread panics or if the channel
/// used for communication between threads is disconnected.
///
/// # Thread Safety
///
/// This function spawns a new thread, so the `work` closure and its return value
/// must be safe to send between threads.
pub fn spawn_future<F, T>(work: F) -> impl FnOnce() -> T
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    let (sender, receiver) = mpsc::channel();

    thread::spawn(move || {
        let result = work();
        sender.send(result).unwrap();
    });

    move || receiver.recv().unwrap()
}

//--------------------------------

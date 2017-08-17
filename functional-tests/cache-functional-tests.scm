(library
  (cache-functional-tests)
  (export register-cache-tests)
  (import (chezscheme)
          (functional-tests)
          (cache-xml)
          (fmt fmt)
          (srfi s8 receive))

  (define-tool cache-check)
  (define-tool cache-dump)
  (define-tool cache-restore)
  (define-tool cache-metadata-size)

  (define (current-metadata) "metadata.bin")

  (define (temp-cache-xml)
    (temp-file-containing (fmt #f (generate-xml 512 1024 128))))

  (define (%with-valid-metadata thunk)
    (cache-restore "-i" (temp-cache-xml) "-o" (current-metadata))
    (thunk))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ body ...)
       (%with-valid-metadata (lambda () body ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define (%with-corrupt-metadata thunk)
    (run-ok "dd if=/dev/zero" (fmt #f "of=" (current-metadata)) "bs=64M count=1")
    (thunk))

  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ body ...) (%with-corrupt-metadata (lambda () body ...)))))


  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-cache-tests) #t)

  (define cache-check-help
    "Usage: cache_check [options] {device|file}
Options:
  {-q|--quiet}
  {-h|--help}
  {-V|--version}
  {--clear-needs-check-flag}
  {--super-block-only}
  {--skip-mappings}
  {--skip-hints}
  {--skip-discards}")

  ;;;-----------------------------------------------------------
  ;;; cache_check scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-check v)
                   "cache_check -V"
                   (receive (stdout _) (cache-check "-V")
                            (assert-equal tools-version stdout)))

  (define-scenario (cache-check version)
                   "cache_check --version"
                   (receive (stdout _) (cache-check "--version")
                            (assert-equal tools-version stdout)))

  (define-scenario (cache-check h)
                   "cache_check -h"
                   (receive (stdout _) (cache-check "-h")
                            (assert-equal cache-check-help stdout)))

  (define-scenario (cache-check help)
                   "cache_check --help"
                   (receive (stdout _) (cache-check "--help")
                            (assert-equal cache-check-help stdout)))

  (define-scenario (cache-check must-specify-metadata)
                   "Metadata file must be specified"
                   (receive (_ stderr) (run-fail "cache_check")
                            (assert-equal
                              (string-append "No input file provided.\n"
                                             cache-check-help)
                              stderr)))

  (define-scenario (cache-check no-such-metadata)
                   "Metadata file doesn't exist."
                   (let ((bad-path "/arbitrary/filename"))
                    (receive (_ stderr) (run-fail "cache_check" bad-path)
                             (assert-starts-with
                               (string-append bad-path ": No such file or directory")
                               stderr))))

  (define-scenario (cache-check metadata-file-cannot-be-a-directory)
                   "Metadata file must not be a directory"
                   (let ((bad-path "/tmp"))
                    (receive (_ stderr) (run-fail "cache_check" bad-path)
                             (assert-starts-with
                               (string-append bad-path ": Not a block device or regular file")
                               stderr))))

  #|
  (define-scenario (cache-check unreadable-metadata)
                   "Metadata file exists, but is unreadable."
                   (with-valid-metadata
                     (run-ok "chmod" "-r" (current-metadata))
                     (receive (_ stderr) (run-fail "cache_check" (current-metadata))
                              (assert-starts-with "syscall 'open' failed: Permission denied" stderr))))
  |#
  (define-scenario (cache-check fails-with-corrupt-metadata)
                   "Fail with corrupt superblock"
                   (with-corrupt-metadata
                     (run-fail "cache_check" (current-metadata))))

  (define-scenario (cache-check failing-q)
                   "Fail quietly with -q"
                   (with-corrupt-metadata
                     (receive (stdout stderr) (run-fail "cache_check" "-q" (current-metadata))
                              (assert-eof stdout)
                              (assert-eof stderr))))
)

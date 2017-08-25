(library
  (cache-functional-tests)
  (export register-cache-tests)
  (import (chezscheme)
          (disk-units)
          (functional-tests)
          (cache-xml)
          (fmt fmt)
          (process)
          (scenario-string-constants)
          (temp-file)
          (srfi s8 receive))

  (define-tool cache-check)
  (define-tool cache-dump)
  (define-tool cache-restore)
  (define-tool cache-metadata-size)

  (define-syntax with-cache-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v (fmt #f (generate-xml 512 1024 128))))
         b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md (meg 4)))
         (with-cache-xml (xml)
           (cache-restore "-i" xml "-o" md)
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md (meg 4)))
         b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-cache-tests) #t)

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
                   (with-corrupt-metadata (md)
                     (run-fail "cache_check" md)))

  (define-scenario (cache-check failing-q)
                   "Fail quietly with -q"
                   (with-corrupt-metadata (md)
                     (receive (stdout stderr) (run-fail "cache_check" "-q" md)
                              (assert-eof stdout)
                              (assert-eof stderr))))

  (define-scenario (cache-check failing-quiet)
                   "Fail quietly with --quiet"
                   (with-corrupt-metadata (md)
                     (receive (stdout stderr) (run-fail "cache_check" "--quiet" md)
                              (assert-eof stdout)
                              (assert-eof stderr))))

  (define-scenario (cache-check valid-metadata-passes)
                   "A valid metadata area passes"
                   (with-valid-metadata (md)
                     (cache-check md)))

  (define-scenario (cache-check deliberately-fail)
                   "remove me"
                   (fail (dsp "bad bad bad")))

  (define-scenario (cache-check bad-metadata-version)
                   "Invalid metadata version fails"
                   (with-cache-xml (xml)
                     (with-temp-file-sized ((md (meg 4)))
                       (cache-restore "-i" xml "-o" md "--debug-override-metadata-version" "12345")
                     (run-fail "cache_check" md))))
)

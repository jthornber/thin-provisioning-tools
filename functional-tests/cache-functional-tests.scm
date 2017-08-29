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
       (with-temp-file-containing ((v "cache.xml" (fmt #f (generate-xml 512 1024 128))))
         b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (meg 4)))
         (with-cache-xml (xml)
           (cache-restore "-i" xml "-o" md)
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (meg 4)))
         b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (meg 4)))
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

  (define-scenario (cache-check unreadable-metadata)
                   "Metadata file exists, but is unreadable."
                   (with-valid-metadata (md)
                     (run-ok "chmod" "-r" md)
                     (receive (_ stderr) (run-fail "cache_check" md)
                              (assert-starts-with "syscall 'open' failed: Permission denied" stderr))))

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

  (define-scenario (cache-check bad-metadata-version)
                   "Invalid metadata version fails"
                   (with-cache-xml (xml)
                     (with-empty-metadata (md)
                       (cache-restore "-i" xml "-o" md "--debug-override-metadata-version" "12345")
                     (run-fail "cache_check" md))))

  ;;;-----------------------------------------------------------
  ;;; cache_restore scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-restore v)
    "print version (-V flag)"
    (receive (stdout _) (cache-restore "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-restore version)
    "print version (--version flags)"
    (receive (stdout _) (cache-restore "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-restore h)
                   "cache_restore -h"
                   (receive (stdout _) (cache-restore "-h")
                            (assert-equal cache-restore-help stdout)))

  (define-scenario (cache-restore help)
                   "cache_restore --help"
                   (receive (stdout _) (cache-restore "--help")
                            (assert-equal cache-restore-help stdout)))

   (define-scenario (cache-restore no-input-file)
                    "forget to specify an input file"
                    (with-empty-metadata (md)
                      (receive (_ stderr) (run-fail "cache_restore" "-o" md)
                        (assert-starts-with "No input file provided." stderr))))

   (define-scenario (cache-restore missing-input-file)
                    "the input file can't be found"
                    (with-empty-metadata (md)
                      (receive (_ stderr) (run-fail "cache_restore -i no-such-file -o" md)
                        (assert-starts-with "Couldn't stat file" stderr))))

   (define-scenario (cache-restore missing-output-file)
                    "the output file can't be found"
                    (with-cache-xml (xml)
                      (receive (_ stderr) (run-fail "cache_restore -i " xml)
                        (assert-starts-with "No output file provided." stderr))))

   (define-scenario (cache-restore tiny-output-file)
                    "Fails if the output file is too small."
                    (with-temp-file-sized ((md "cache.bin" (* 1024 4)))
                      (with-cache-xml (xml)
                        (receive (_ stderr) (run-fail "cache_restore" "-i" xml "-o" md)
                          (assert-starts-with cache-restore-outfile-too-small-text stderr)))))

   (define-scenario (cache-restore successfully-restores)
     "Restore succeeds."
     (with-empty-metadata (md)
       (with-cache-xml (xml)
         (cache-restore "-i" xml "-o" md))))

   (define-scenario (cache-restore q)
                    "cache_restore accepts -q"
                    (with-empty-metadata (md)
                      (with-cache-xml (xml)
                        (receive (stdout stderr) (cache-restore "-i" xml "-o" md "-q")
                          (assert-eof stdout)
                          (assert-eof stderr)))))

   (define-scenario (cache-restore quiet)
                    "cache_restore accepts --quiet"
                    (with-empty-metadata (md)
                      (with-cache-xml (xml)
                        (receive (stdout stderr) (cache-restore "-i" xml "-o" md "--quiet")
                          (assert-eof stdout)
                          (assert-eof stderr)))))

   ;; Failing due to a genuine bug in cache_restore
   (define-scenario (cache-restore override-metadata-version)
     "we can set any metadata version"
     (with-empty-metadata (md)
       (with-cache-xml (xml)
         (cache-restore "-i" xml "-o" md "--debug-override-metadata-version 10298"))))

   (define-scenario (cache-restore omit-clean-shutdown)
     "accepts --omit-clean-shutdown"
     (with-empty-metadata (md)
       (with-cache-xml (xml)
         (cache-restore "-i" xml "-o" md "--omit-clean-shutdown"))))

  ;;;-----------------------------------------------------------
  ;;; cache_dump scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-dump v)
    "print version (-V flag)"
    (receive (stdout _) (cache-dump "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-dump version)
    "print version (--version flags)"
    (receive (stdout _) (cache-dump "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-dump h)
                   "cache_dump -h"
                   (receive (stdout _) (cache-dump "-h")
                            (assert-equal cache-dump-help stdout)))

  (define-scenario (cache-dump help)
                   "cache_dump --help"
                   (receive (stdout _) (cache-dump "--help")
                            (assert-equal cache-dump-help stdout)))

  (define-scenario (cache-dump missing-input-file)
    "Fails with missing input file."
    (receive (stdout stderr) (run-fail "cache_dump")
      (assert-starts-with "No input file provided." stderr)))

  (define-scenario (cache-dump restore-is-noop)
                    "cache_dump followed by cache_restore is a noop."
                    (with-valid-metadata (md)
                      (receive (d1-stdout _) (cache-dump md)
                        (with-temp-file-containing ((xml "cache.xml" d1-stdout))
                          (cache-restore "-i" xml "-o" md)
                          (receive (d2-stdout _) (cache-dump md)
                            (assert-equal d1-stdout d2-stdout))))))
)

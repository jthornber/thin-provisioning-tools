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
  (define-tool cache-repair)

  (define-syntax with-cache-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "cache.xml" (fmt #f (generate-xml 512 1024 128))))
         b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
         (with-cache-xml (xml)
           (run-ok (cache-restore "-i" xml "-o" md))
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
         (system (fmt #f "dd if=/usr/bin/ls of=" md " bs=4096 > /dev/null 2>&1"))
           b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "cache.bin" (to-bytes (meg 4))))
                            b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-cache-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; cache_dump scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-dump small-input-file)
    "Fails with small input file"
    (with-temp-file-sized ((md "cache.bin" 512))
      (run-fail
        (cache-dump md))))

  ;;;-----------------------------------------------------------
  ;;; cache_metadata_size scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (cache-metadata-size v)
    "cache_metadata_size -V"
    (run-ok-rcv (stdout _) (cache-metadata-size "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-metadata-size version)
    "cache_metadata_size --version"
    (run-ok-rcv (stdout _) (cache-metadata-size "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (cache-metadata-size h)
    "cache_metadata_size -h"
    (run-ok-rcv (stdout _) (cache-metadata-size "-h")
      (assert-equal cache-metadata-size-help stdout)))

  (define-scenario (cache-metadata-size help)
    "cache_metadata_size --help"
    (run-ok-rcv (stdout _) (cache-metadata-size "--help")
      (assert-equal cache-metadata-size-help stdout)))

  (define-scenario (cache-metadata-size no-args)
    "No arguments specified causes fail"
    (run-fail-rcv (_ stderr) (cache-metadata-size)
      (assert-equal "Please specify either --device-size and --block-size, or --nr-blocks."
                    stderr)))

  (define-scenario (cache-metadata-size device-size-only)
    "Just --device-size causes fail"
    (run-fail-rcv (_ stderr) (cache-metadata-size "--device-size" (to-bytes (meg 100)))
      (assert-equal "If you specify --device-size you must also give --block-size."
                    stderr)))

  (define-scenario (cache-metadata-size block-size-only)
    "Just --block-size causes fail"
    (run-fail-rcv (_ stderr) (cache-metadata-size "--block-size" 128)
      (assert-equal "If you specify --block-size you must also give --device-size."
                    stderr)))

  (define-scenario (cache-metadata-size conradictory-info-fails)
    "Contradictory info causes fail"
    (run-fail-rcv (_ stderr) (cache-metadata-size "--device-size 102400 --block-size 1000 --nr-blocks 6")
      (assert-equal "Contradictory arguments given, --nr-blocks doesn't match the --device-size and --block-size."
                    stderr)))

  (define-scenario (cache-metadata-size all-args-agree)
    "All args agreeing succeeds"
    (run-ok-rcv (stdout stderr) (cache-metadata-size "--device-size" 102400 "--block-size" 100 "--nr-blocks" 1024)
      (assert-equal "8248 sectors" stdout)
      (assert-eof stderr)))

  (define-scenario (cache-metadata-size nr-blocks-alone)
    "Just --nr-blocks succeeds"
    (run-ok-rcv (stdout stderr) (cache-metadata-size "--nr-blocks" 1024)
      (assert-equal "8248 sectors" stdout)
      (assert-eof stderr)))

  (define-scenario (cache-metadata-size dev-size-and-block-size-succeeds)
    "Specifying --device-size with --block-size succeeds"
    (run-ok-rcv (stdout stderr) (cache-metadata-size "--device-size" 102400 "--block-size" 100)
      (assert-equal "8248 sectors" stdout)
      (assert-eof stderr)))

  (define-scenario (cache-metadata-size big-config)
    "A big configuration succeeds"
    (run-ok-rcv (stdout stderr) (cache-metadata-size "--nr-blocks 67108864")
      (assert-equal "3678208 sectors" stdout)
      (assert-eof stderr)))
)

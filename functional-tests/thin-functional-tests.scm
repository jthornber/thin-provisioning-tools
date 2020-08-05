(library
  (thin-functional-tests)

  (export register-thin-tests)

  (import
    (chezscheme)
    (bcache block-manager)
    (disk-units)
    (fmt fmt)
    (functional-tests)
    (process)
    (scenario-string-constants)
    (temp-file)
    (thin xml)
    (srfi s8 receive))

  (define-tool thin-check)
  (define-tool thin-delta)
  (define-tool thin-dump)
  (define-tool thin-restore)
  (define-tool thin-rmap)
  (define-tool thin-repair)
  (define-tool thin-metadata-pack)
  (define-tool thin-metadata-unpack)

  (define-syntax with-thin-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "thin.xml" (fmt #f (generate-xml 10 1000))))
                                  b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
         (with-thin-xml (xml)
           (run-ok (thin-restore "-i" xml "-o" md))
           b1 b2 ...)))))

  ;;; It would be nice if the metadata was at least similar to valid data.
  ;;; Here I'm just using the start of the ls binary as 'random' data.
  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
         (system (fmt #f "dd if=/usr/bin/ls of=" md " bs=4096 > /dev/null 2>&1"))
         b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "thin.bin" (meg 4)))
                             b1 b2 ...))))

  (define (damage-superblock md)
    (system (string-append "dd if=/dev/zero of=" md " bs=4K count=1 conv=notrunc > /dev/null 2>&1")))

  (define-syntax with-damaged-superblock
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-valid-metadata (md)
         (damage-superblock md)
         b1 b2 ...))))

  ;; We have to export something that forces all the initialisation expressions
  ;; to run.
  (define (register-thin-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; thin_metadata_pack scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-metadata-pack version)
    "accepts --version"
    (run-ok-rcv (stdout _) (thin-metadata-pack "--version")
      (assert-equal "thin_metadata_pack 0.9.0-rc2" stdout)))

  (define-scenario (thin-metadata-pack h)
    "accepts -h"
    (run-ok-rcv (stdout _) (thin-metadata-pack "-h")
      (assert-equal thin-metadata-pack-help stdout)))

  (define-scenario (thin-metadata-pack help)
    "accepts --help"
    (run-ok-rcv (stdout _) (thin-metadata-pack "--help")
      (assert-equal thin-metadata-pack-help stdout)))

  (define-scenario (thin-metadata-pack unrecognised-option)
    "Unrecognised option should cause failure"
    (with-valid-metadata (md)
      (run-fail-rcv (stdout stderr) (thin-metadata-pack "--unleash-the-hedgehogs")
        (assert-starts-with "error: Found argument '--unleash-the-hedgehogs'" stderr)))) 

  (define-scenario (thin-metadata-pack missing-input-file)
    "the input file wasn't specified"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-pack "-o " md)
        (assert-starts-with "error: The following required arguments were not provided:\n    -i <DEV>" stderr))))

  (define-scenario (thin-metadata-pack no-such-input-file)
    "the input file can't be found"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-pack "-i no-such-file -o" md)
        (assert-starts-with "Couldn't find input file" stderr))))

  (define-scenario (thin-metadata-pack missing-output-file)
    "the output file wasn't specified"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-pack "-i" md)
        (assert-starts-with "error: The following required arguments were not provided:\n    -o <FILE>" stderr))))

  ;;;-----------------------------------------------------------
  ;;; thin_metadata_unpack scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (thin-metadata-unpack version)
    "accepts --version"
    (run-ok-rcv (stdout _) (thin-metadata-unpack "--version")
      (assert-equal "thin_metadata_unpack 0.9.0-rc2" stdout)))

  (define-scenario (thin-metadata-unpack h)
    "accepts -h"
    (run-ok-rcv (stdout _) (thin-metadata-unpack "-h")
      (assert-equal thin-metadata-unpack-help stdout)))

  (define-scenario (thin-metadata-unpack help)
    "accepts --help"
    (run-ok-rcv (stdout _) (thin-metadata-unpack "--help")
      (assert-equal thin-metadata-unpack-help stdout)))

  (define-scenario (thin-metadata-unpack unrecognised-option)
    "Unrecognised option should cause failure"
    (with-valid-metadata (md)
      (run-fail-rcv (stdout stderr) (thin-metadata-unpack "--unleash-the-hedgehogs")
        (assert-starts-with "error: Found argument '--unleash-the-hedgehogs'" stderr)))) 

  (define-scenario (thin-metadata-unpack missing-input-file)
    "the input file wasn't specified"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-unpack "-o " md)
        (assert-starts-with "error: The following required arguments were not provided:\n    -i <DEV>" stderr))))

  (define-scenario (thin-metadata-unpack no-such-input-file)
    "the input file can't be found"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-unpack "-i no-such-file -o" md)
        (assert-starts-with "Couldn't find input file" stderr))))

  (define-scenario (thin-metadata-unpack missing-output-file)
    "the output file wasn't specified"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-metadata-unpack "-i" md)
        (assert-starts-with "error: The following required arguments were not provided:\n    -o <FILE>" stderr))))

  (define-scenario (thin-metadata-unpack garbage-input-file)
    "the input file is just zeroes"
    (with-empty-metadata (bad-pack)
      (run-fail-rcv (_ stderr) (thin-metadata-unpack "-i " bad-pack "-o junk")
        (assert-starts-with "Not a pack file." stderr))))

  ;;;-----------------------------------------------------------
  ;;; thin_metadata_pack/unpack end to end scenario
  ;;;-----------------------------------------------------------)
  (define-scenario (thin-metadata-pack end-to-end)
    "pack -> unpack recovers metadata"
    (let ((pack-file "md.pack"))
      (with-valid-metadata (md-in)
        (with-empty-metadata (md-out)
          (run-ok (thin-metadata-pack "-i" md-in "-o" pack-file))
          (run-ok (thin-metadata-unpack "-i" pack-file "-o" md-out))
            (run-ok-rcv (dump1 _) (thin-dump md-in)
              (run-ok-rcv (dump2 _) (thin-dump md-out)
                (assert-equal dump1 dump2)))))))

)

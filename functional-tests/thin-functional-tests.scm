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
  ;;; thin_delta scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (thin-delta v)
    "thin_delta accepts -V"
    (run-ok-rcv (stdout _) (thin-delta "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-delta version)
    "thin_delta accepts --version"
    (run-ok-rcv (stdout _) (thin-delta "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-delta h)
    "thin_delta accepts -h"
    (run-ok-rcv (stdout _) (thin-delta "-h")
      (assert-equal thin-delta-help stdout)))

  (define-scenario (thin-delta help)
    "thin_delta accepts --help"
    (run-ok-rcv (stdout _) (thin-delta "--help")
      (assert-equal thin-delta-help stdout)))

  (define-scenario (thin-delta unrecognised-option)
    "Unrecognised option should cause failure"
    (with-valid-metadata (md)
      (run-fail-rcv (stdout stderr) (thin-delta "--unleash-the-hedgehogs")
        (assert-matches ".*thin_delta: unrecognized option '--unleash-the-hedgehogs" stderr))))

  (define-scenario (thin-delta snap1-unspecified)
    "Fails without --snap1 fails"
    (run-fail-rcv (_ stderr) (thin-delta "--snap2 45 foo")
      (assert-starts-with "--snap1 not specified." stderr)))

  (define-scenario (thin-delta snap2-unspecified)
    "Fails without --snap2 fails"
    (run-fail-rcv (_ stderr) (thin-delta "--snap1 45 foo")
      (assert-starts-with "--snap2 not specified." stderr)))

  (define-scenario (thin-delta device-unspecified)
    "Fails if no device given"
    (run-fail-rcv (_ stderr) (thin-delta "--snap1 45 --snap2 46")
      (assert-starts-with "No input device provided." stderr)))

  ;;;-----------------------------------------------------------
  ;;; thin_repair scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (thin-repair dont-repair-xml)
    "Fails gracefully if run on XML rather than metadata"
    (with-thin-xml (xml)
      (with-empty-metadata (md)
        (run-fail-rcv (_ stderr) (thin-repair "-i" xml "-o" md)
          #t))))

  (define-scenario (thin-repair missing-input-file)
    "the input file can't be found"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-repair "-i no-such-file -o" md)
        (assert-superblock-all-zeroes md)
        (assert-starts-with "Couldn't stat file" stderr))))

  (define-scenario (thin-repair garbage-input-file)
    "the input file is just zeroes"
    (with-empty-metadata (md1)
      (with-corrupt-metadata (md2)
        (run-fail-rcv (_ stderr) (thin-repair "-i " md1 "-o" md2)
          (assert-superblock-all-zeroes md2)))))

  (define-scenario (thin-repair missing-output-file)
    "the output file can't be found"
    (with-thin-xml (xml)
      (run-fail-rcv (_ stderr) (thin-repair "-i " xml)
        (assert-starts-with "No output file provided." stderr))))

  (define-scenario (thin-repair override transaction-id)
    "thin_repair obeys the --transaction-id override"
    (with-valid-metadata (md1)
      (with-empty-metadata (md2)
        (run-ok-rcv (stdout stderr) (thin-repair "--transaction-id 2345" "-i" md1 "-o" md2)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md2)
          (assert-matches ".*transaction=\"2345\"" stdout)))))

  (define-scenario (thin-repair override data-block-size)
    "thin_repair obeys the --data-block-size override"
    (with-valid-metadata (md1)
      (with-empty-metadata (md2)
        (run-ok-rcv (stdout stderr) (thin-repair "--data-block-size 8192" "-i" md1 "-o" md2)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md2)
          (assert-matches ".*data_block_size=\"8192\"" stdout)))))

  (define-scenario (thin-repair override nr-data-blocks)
    "thin_repair obeys the --nr-data-blocks override"
    (with-valid-metadata (md1)
      (with-empty-metadata (md2)
        (run-ok-rcv (stdout stderr) (thin-repair "--nr-data-blocks 234500" "-i" md1 "-o" md2)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md2)
          (assert-matches ".*nr_data_blocks=\"234500\"" stdout)))))

  (define-scenario (thin-repair superblock succeeds)
    "thin_repair can restore a missing superblock"
    (with-valid-metadata (md1)
      (run-ok-rcv (expected-xml stderr) (thin-dump "--transaction-id=5" "--data-block-size=128" "--nr-data-blocks=4096000" md1)
        (damage-superblock md1)
        (with-empty-metadata (md2)
          (run-ok-rcv (_ stderr) (thin-repair "--transaction-id=5" "--data-block-size=128" "--nr-data-blocks=4096000" "-i" md1 "-o" md2)
            (assert-eof stderr))
          (run-ok-rcv (repaired-xml stderr) (thin-dump md2)
            (assert-eof stderr)
            (assert-equal expected-xml repaired-xml))))))

  (define-scenario (thin-repair superblock missing-transaction-id)
    "--transaction-id is mandatory if the superblock is damaged"
    (with-damaged-superblock (md1)
      (with-empty-metadata (md2)
        (run-fail-rcv (_ stderr) (thin-repair "--data-block-size=128" "--nr-data-blocks=4096000" "-i" md1 "-o" md2)
          (assert-matches ".*transaction id.*" stderr)))))

  (define-scenario (thin-repair superblock missing-data-block-size)
    "--data-block-size is mandatory if the superblock is damaged"
    (with-damaged-superblock (md1)
      (with-empty-metadata (md2)
        (run-fail-rcv (_ stderr) (thin-repair "--transaction-id=5" "--nr-data-blocks=4096000" "-i" md1 "-o" md2)
          (assert-matches ".*data block size.*" stderr)))))

  (define-scenario (thin-repair superblock missing-nr-data-blocks)
    "--nr-data-blocks is mandatory if the superblock is damaged"
    (with-damaged-superblock (md1)
      (with-empty-metadata (md2)
        (run-fail-rcv (_ stderr) (thin-repair "--transaction-id=5" "--data-block-size=128" "-i" md1 "-o" md2)
          (assert-matches ".*nr data blocks.*" stderr)))))


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

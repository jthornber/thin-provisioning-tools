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
  ;;; thin_check scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-check v)
    "thin_check -V"
    (run-ok-rcv (stdout _) (thin-check "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-check version)
    "thin_check --version"
    (run-ok-rcv (stdout _) (thin-check "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-check h)
    "print help (-h)"
    (run-ok-rcv (stdout _) (thin-check "-h")
      (assert-equal thin-check-help stdout)))

  (define-scenario (thin-check help)
    "print help (--help)"
    (run-ok-rcv (stdout _) (thin-check "--help")
      (assert-equal thin-check-help stdout)))

  (define-scenario (thin-check bad-option)
    "Unrecognised option should cause failure"
    (run-fail (thin-check "--hedgehogs-only")))

  (define-scenario (thin-check superblock-only-valid)
    "--super-block-only check passes on valid metadata"
    (with-valid-metadata (md)
      (run-ok (thin-check "--super-block-only" md))))

  (define-scenario (thin-check superblock-only-invalid)
    "--super-block-only check fails with corrupt metadata"
    (with-corrupt-metadata (md)
      (run-fail (thin-check "--super-block-only" md))))

  (define-scenario (thin-check skip-mappings-valid)
    "--skip-mappings check passes on valid metadata"
    (with-valid-metadata (md)
      (run-ok (thin-check "--skip-mappings" md))))

  (define-scenario (thin-check ignore-non-fatal-errors)
    "--ignore-non-fatal-errors check passes on valid metadata"
    (with-valid-metadata (md)
      (run-ok (thin-check "--ignore-non-fatal-errors" md))))

  (define-scenario (thin-check quiet)
    "--quiet should give no output"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-check "--quiet" md)
        (assert-eof stdout)
        (assert-eof stderr))))

  (define-scenario (thin-check clear-needs-check-flag)
    "Accepts --clear-needs-check-flag"
    (with-valid-metadata (md)
      (run-ok (thin-check "--clear-needs-check-flag" md))))

  (define-scenario (thin-check tiny-metadata)
    "Prints helpful message in case tiny metadata given"
    (with-temp-file-sized ((md "thin.bin" 1024))
      (run-fail-rcv (_ stderr) (thin-check md)
        (assert-starts-with "Metadata device/file too small.  Is this binary metadata?" stderr))))

  (define-scenario (thin-check spot-accidental-xml-data)
    "Prints helpful message if XML metadata given"
    (with-thin-xml (xml)
      (system (fmt #f "man bash >> " xml))
      (run-fail-rcv (_ stderr) (thin-check xml)
        (assert-matches ".*This looks like XML.  thin_check only checks the binary metadata format." stderr))))

  (define-scenario (thin-check info-fields)
    "Outputs info fields"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-check md)
        (assert-matches ".*TRANSACTION_ID=[0-9]+.*" stdout)
	(assert-matches ".*METADATA_FREE_BLOCKS=[0-9]+.*" stdout))))

  ;;;-----------------------------------------------------------
  ;;; thin_restore scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-restore print-version-v)
    "print help (-V)"
    (run-ok-rcv (stdout _) (thin-restore "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-restore print-version-long)
    "print help (--version)"
    (run-ok-rcv (stdout _) (thin-restore "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-restore h)
    "print help (-h)"
    (run-ok-rcv (stdout _) (thin-restore "-h")
      (assert-equal thin-restore-help stdout)))

  (define-scenario (thin-restore help)
    "print help (-h)"
    (run-ok-rcv (stdout _) (thin-restore "--help")
      (assert-equal thin-restore-help stdout)))

  (define-scenario (thin-restore no-input-file)
    "forget to specify an input file"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-restore "-o" md)
        (assert-starts-with "No input file provided." stderr))))

  (define-scenario (thin-restore missing-input-file)
    "the input file can't be found"
    (with-empty-metadata (md)
      (run-fail-rcv (_ stderr) (thin-restore "-i no-such-file -o" md)
        (assert-superblock-all-zeroes md)
        (assert-starts-with "Couldn't stat file" stderr))))

  (define-scenario (thin-restore garbage-input-file)
    "the input file is just zeroes"
    (with-empty-metadata (md)
      (with-temp-file-sized ((xml "thin.xml" 4096))
        (run-fail-rcv (_ stderr) (thin-restore "-i " xml "-o" md)
          (assert-superblock-all-zeroes md)))))

  (define-scenario (thin-restore missing-output-file)
    "the output file can't be found"
    (with-thin-xml (xml)
      (run-fail-rcv (_ stderr) (thin-restore "-i " xml)
        (assert-starts-with "No output file provided." stderr))))

  (define-scenario (thin-restore tiny-output-file)
    "Fails if the output file is too small."
    (with-temp-file-sized ((md "thin.bin" 4096))
      (with-thin-xml (xml)
        (run-fail-rcv (_ stderr) (thin-restore "-i" xml "-o" md)
          (assert-starts-with thin-restore-outfile-too-small-text stderr)))))

  (define-scenario (thin-restore q)
    "thin_restore accepts -q"
    (with-empty-metadata (md)
      (with-thin-xml (xml)
        (run-ok-rcv (stdout _) (thin-restore "-i" xml "-o" md "-q")
          (assert-eof stdout)))))

  (define-scenario (thin-restore quiet)
    "thin_restore accepts --quiet"
    (with-empty-metadata (md)
      (with-thin-xml (xml)
        (run-ok-rcv (stdout _) (thin-restore "-i" xml "-o" md "--quiet")
          (assert-eof stdout)))))

  (define-scenario (thin-restore override transaction-id)
    "thin_restore obeys the --transaction-id override"
    (with-empty-metadata (md)
      (with-thin-xml (xml)
        (run-ok-rcv (stdout stderr) (thin-restore "--transaction-id 2345" "-i" xml "-o" md)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md)
          (assert-matches ".*transaction=\"2345\"" stdout)))))

  (define-scenario (thin-restore override data-block-size)
    "thin_restore obeys the --data-block-size override"
    (with-empty-metadata (md)
      (with-thin-xml (xml)
        (run-ok-rcv (stdout stderr) (thin-restore "--data-block-size 8192" "-i" xml "-o" md)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md)
          (assert-matches ".*data_block_size=\"8192\"" stdout)))))

  (define-scenario (thin-restore override nr-data-blocks)
    "thin_restore obeys the --nr-data-blocks override"
    (with-empty-metadata (md)
      (with-thin-xml (xml)
        (run-ok-rcv (stdout stderr) (thin-restore "--nr-data-blocks 234500" "-i" xml "-o" md)
          (assert-eof stderr))
        (run-ok-rcv (stdout stderr) (thin-dump md)
          (assert-matches ".*nr_data_blocks=\"234500\"" stdout)))))

  ;;;-----------------------------------------------------------
  ;;; thin_dump scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-dump small-input-file)
    "Fails with small input file"
    (with-temp-file-sized ((md "thin.bin" 512))
      (run-fail (thin-dump md))))

  (define-scenario (thin-dump restore-is-noop)
    "thin_dump followed by thin_restore is a noop."
    (with-valid-metadata (md)
      (run-ok-rcv (d1-stdout _) (thin-dump md)
        (with-temp-file-containing ((xml "thin.xml" d1-stdout))
          (run-ok (thin-restore "-i" xml "-o" md))
          (run-ok-rcv (d2-stdout _) (thin-dump md)
            (assert-equal d1-stdout d2-stdout))))))

  (define-scenario (thin-dump no-stderr)
    "thin_dump of clean data does not output error messages to stderr"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-dump md)
        (assert-eof stderr))))

  (define-scenario (thin-dump override transaction-id)
    "thin_dump obeys the --transaction-id override"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-dump "--transaction-id 2345" md)
        (assert-eof stderr)
        (assert-matches ".*transaction=\"2345\"" stdout))))

  (define-scenario (thin-dump override data-block-size)
    "thin_dump obeys the --data-block-size override"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-dump "--data-block-size 8192" md)
        (assert-eof stderr)
        (assert-matches ".*data_block_size=\"8192\"" stdout))))

  (define-scenario (thin-dump override nr-data-blocks)
    "thin_dump obeys the --nr-data-blocks override"
    (with-valid-metadata (md)
      (run-ok-rcv (stdout stderr) (thin-dump "--nr-data-blocks 234500" md)
        (assert-eof stderr)
        (assert-matches ".*nr_data_blocks=\"234500\"" stdout))))

  (define-scenario (thin-dump repair-superblock succeeds)
    "thin_dump can restore a missing superblock"
    (with-valid-metadata (md)
      (run-ok-rcv (expected-xml stderr) (thin-dump "--transaction-id=5" "--data-block-size=128" "--nr-data-blocks=4096000" md)
        (damage-superblock md)
        (run-ok-rcv (repaired-xml stderr) (thin-dump "--repair" "--transaction-id=5" "--data-block-size=128" "--nr-data-blocks=4096000" md)
          (assert-eof stderr)
          (assert-equal expected-xml repaired-xml)))))

  (define-scenario (thin-dump repair-superblock missing-transaction-id)
    "--transaction-id is mandatory if the superblock is damaged"
    (with-damaged-superblock (md)
      (run-fail-rcv (_ stderr) (thin-dump "--repair" "--data-block-size=128" "--nr-data-blocks=4096000" md)
        (assert-matches ".*transaction id.*" stderr))))

  (define-scenario (thin-dump repair-superblock missing-data-block-size)
    "--data-block-size is mandatory if the superblock is damaged"
    (with-damaged-superblock (md)
      (run-fail-rcv (_ stderr) (thin-dump "--repair" "--transaction-id=5" "--nr-data-blocks=4096000" md)
        (assert-matches ".*data block size.*" stderr))))

  (define-scenario (thin-dump repair-superblock missing-nr-data-blocks)
    "--nr-data-blocks is mandatory if the superblock is damaged"
    (with-damaged-superblock (md)
      (run-fail-rcv (_ stderr) (thin-dump "--repair" "--transaction-id=5" "--data-block-size=128" md)
        (assert-matches ".*nr data blocks.*" stderr))))

  ;;;-----------------------------------------------------------
  ;;; thin_rmap scenarios
  ;;;-----------------------------------------------------------

  (define-scenario (thin-rmap v)
    "thin_rmap accepts -V"
    (run-ok-rcv (stdout _) (thin-rmap "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-rmap version)
    "thin_rmap accepts --version"
    (run-ok-rcv (stdout _) (thin-rmap "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (thin-rmap h)
    "thin_rmap accepts -h"
    (run-ok-rcv (stdout _) (thin-rmap "-h")
      (assert-equal thin-rmap-help stdout)))

  (define-scenario (thin-rmap help)
    "thin_rmap accepts --help"
    (run-ok-rcv (stdout _) (thin-rmap "--help")
      (assert-equal thin-rmap-help stdout)))

  (define-scenario (thin-rmap unrecognised-flag)
    "thin_rmap complains with bad flags."
    (run-fail (thin-rmap "--unleash-the-hedgehogs")))

  (define-scenario (thin-rmap valid-region-format-should-pass)
    "thin_rmap with a valid region format should pass."
    (with-valid-metadata (md)
      (run-ok
        (thin-rmap "--region 23..7890" md))))

  (define-scenario (thin-rmap invalid-region-should-fail)
    "thin_rmap with an invalid region format should fail."
    (for-each (lambda (pattern)
                      (with-valid-metadata (md)
                        (run-fail (thin-rmap "--region" pattern md))))
              '("23,7890" "23..six" "found..7890" "89..88" "89..89" "89.." "" "89...99")))

  (define-scenario (thin-rmap multiple-regions-should-pass)
    "thin_rmap should handle multiple regions."
    (with-valid-metadata (md)
      (run-ok (thin-rmap "--region 1..23 --region 45..78" md))))

  (define-scenario (thin-rmap handles-junk-input)
    "Fail gracefully if given nonsense"
    (with-thin-xml (xml)
      (run-fail-rcv (_ stderr) (thin-rmap "--region 0..-1" xml)
          #t)))

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
      (assert-equal "thin_metadata_pack 0.8.5" stdout)))

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
      (assert-equal "thin_metadata_unpack 0.8.5" stdout)))

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

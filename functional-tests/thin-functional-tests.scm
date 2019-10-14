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
  )

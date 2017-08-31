(library
  (era-functional-tests)
  (export register-era-tests)
  (import (chezscheme)
          (disk-units)
          (functional-tests)
          (era-xml)
          (fmt fmt)
          (process)
          (scenario-string-constants)
          (temp-file)
          (srfi s8 receive))

  (define-tool era-check)
  (define-tool era-restore)
  (define-tool era-dump)

  (define-syntax with-era-xml
    (syntax-rules ()
      ((_ (v) b1 b2 ...)
       (with-temp-file-containing ((v "era.xml" (fmt #f (generate-xml 128 256 32 4))))
                                  b1 b2 ...))))

  (define-syntax with-valid-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
                             (with-era-xml (xml)
                                           (era-restore "-i" xml "-o" md)
                                           b1 b2 ...)))))

  (define-syntax with-corrupt-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
                             b1 b2 ...))))

  (define-syntax with-empty-metadata
    (syntax-rules ()
      ((_ (md) b1 b2 ...)
       (with-temp-file-sized ((md "era.bin" (meg 4)))
                             b1 b2 ...))))

  (define (register-era-tests) #t)

  ;;;-----------------------------------------------------------
  ;;; era_check scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (era-check v)
    "era_check -V"
    (receive (stdout _) (era-check "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (era-check version)
    "era_check --version"
    (receive (stdout _) (era-check "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (era-check h)
    "era_check -h"
    (receive (stdout _) (era-check "-h")
      (assert-equal era-check-help stdout)))

  (define-scenario (era-check help)
    "era_check --help"
    (receive (stdout _) (era-check "--help")
      (assert-equal era-check-help stdout)))

  (define-scenario (era-check no-device-specified)
    "Fail if no device specified"
    (receive (_ stderr) (run-fail "era_check")
      (assert-starts-with "No input file provided." stderr)))

  (define-scenario (era-check dev-not-exist)
    "Fail if specified device doesn't exist"
    (receive (_ stderr) (run-fail "era_check /dev/unlikely")
      (assert-starts-with "/dev/unlikely: No such file or directory" stderr)))

  (define-scenario (era-check dev-is-a-directory)
    "Fail if given a directory instead of a file or device"
    (receive (_ stderr) (run-fail "era_check /tmp")
      (assert-starts-with "/tmp: Not a block device or regular file" stderr)))

  (define-scenario (era-check bad-permissions)
    "Fail if given a device with inadequate access permissions"
    (with-temp-file-sized ((md "era.bin" (meg 4)))
      (run-ok "chmod -r" md)
      (receive (_ stderr) (run-fail "era_check" md)
        (assert-starts-with "syscall 'open' failed: Permission denied" stderr))))

  (define-scenario (era-check empty-dev)
    "Fail if given a file of zeroes"
    (with-empty-metadata (md)
      (run-fail "era_check" md)))

  (define-scenario (era-check quiet)
    "Fail should give no output if --quiet"
    (with-empty-metadata (md)
      (receive (stdout stderr) (run-fail "era_check --quiet" md)
        (assert-eof stdout)
        (assert-eof stderr))))

  (define-scenario (era-check q)
    "Fail should give no output if -q"
    (with-empty-metadata (md)
      (receive (stdout stderr) (run-fail "era_check -q" md)
        (assert-eof stdout)
        (assert-eof stderr))))

  ;;;-----------------------------------------------------------
  ;;; era_restore scenarios
  ;;;-----------------------------------------------------------
  (define-scenario (era-restore v)
    "era_restore -V"
    (receive (stdout _) (era-restore "-V")
      (assert-equal tools-version stdout)))

  (define-scenario (era-restore version)
    "era_restore --version"
    (receive (stdout _) (era-restore "--version")
      (assert-equal tools-version stdout)))

  (define-scenario (era-restore h)
    "era_restore -h"
    (receive (stdout _) (era-restore "-h")
      (assert-equal era-restore-help stdout)))

  (define-scenario (era-restore help)
    "era_restore --help"
    (receive (stdout _) (era-restore "--help")
      (assert-equal era-restore-help stdout)))

  (define-scenario (era-restore input-unspecified)
    "Fails if no xml specified"
    (with-empty-metadata (md)
      (receive (_ stderr) (run-fail "era_restore" "-o" md)
        (assert-starts-with "No input file provided." stderr))))

  (define-scenario (era-restore output-unspecified)
    "Fails if no metadata dev specified"
    (with-era-xml (xml)
      (receive (_ stderr) (run-fail "era_restore" "-i" xml)
        (assert-starts-with "No output file provided." stderr))))

  (define-scenario (era-restore success)
    "Succeeds with xml and metadata"
    (with-era-xml (xml)
      (with-empty-metadata (md)
        (era-restore "-i" xml "-o" md))))

  (define-scenario (era-restore quiet)
    "No output with --quiet (succeeding)"
    (with-era-xml (xml)
      (with-empty-metadata (md)
        (receive (stdout stderr) (era-restore "--quiet" "-i" xml "-o" md)
          (assert-eof stdout)
          (assert-eof stderr)))))

  (define-scenario (era-restore q)
    "No output with -q (succeeding)"
    (with-era-xml (xml)
      (with-empty-metadata (md)
        (receive (stdout stderr) (era-restore "-q" "-i" xml "-o" md)
          (assert-eof stdout)
          (assert-eof stderr)))))

  (define-scenario (era-restore quiet-fail)
    "No output with --quiet (failing)"
    (with-temp-file ((bad-xml "era.xml"))
      (with-empty-metadata (md)
        (receive (stdout stderr) (run-fail "era_restore" "--quiet" "-i" bad-xml "-o" md)
          (assert-eof stdout)
          (assert-eof stderr)))))

  (define-scenario (era-restore q-fail)
    "No output with --q(failing)"
    (with-temp-file ((bad-xml "era.xml"))
      (with-empty-metadata (md)
        (receive (stdout stderr) (run-fail "era_restore" "-q" "-i" bad-xml "-o" md)
          (assert-eof stdout)
          (assert-eof stderr)))))

  (define-scenario (era-dump restore-is-noop)
    "era_dump followed by era_restore is a noop."
    (with-valid-metadata (md)
      (receive (d1-stdout _) (era-dump md)
        (with-temp-file-containing ((xml "era.xml" d1-stdout))
          (era-restore "-i" xml "-o" md)
          (receive (d2-stdout _) (era-dump md)
            (assert-equal d1-stdout d2-stdout))))))
  )

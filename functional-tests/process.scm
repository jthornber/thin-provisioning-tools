(library
  (process)

  (export run
          run-ok
          run-fail)

  (import (chezscheme)
          (fail)
          (fmt fmt)
          (logging)
          (list-utils)
          (srfi s8 receive)
          (temp-file)
          (utils))

  ;;;--------------------------------------------------------------------
  ;;; Run a sub process and capture it's output.
  ;;; Ideally we'd use open-process-ports, but that loses us the exit code which
  ;;; we need for testing.  So we use system, and redirect stderr and stdout to
  ;;; temporary files, and subsequently read them in.  Messy, but fine for tests.

  (define (build-command-line cmd-and-args)
    (apply fmt #f (map dsp (intersperse " " cmd-and-args))))

  (define (run . cmd-and-args)
    (with-temp-file ((stdout-file "stdout")
                     (stderr-file "stderr"))
      (let* ((short-cmd (build-command-line cmd-and-args))
             (cmd (fmt #f (dsp (build-command-line cmd-and-args))
                       (dsp " > ")
                       (dsp stdout-file)
                       (dsp " 2> ")
                       (dsp stderr-file))))
        (info (dsp "cmd: ") (dsp short-cmd))
        (let ((exit-code (system cmd)))
         (let ((out (slurp-file stdout-file))
               (err (slurp-file stderr-file)))
           (info (dsp "stdout: ") (dsp out))
           (info (dsp "stderr: ") (dsp err))
           (values exit-code out err))))))

  (define (run-with-exit-code pred cmd-and-args)
    (receive (exit-code stdout stderr) (apply run cmd-and-args)
             (if (pred exit-code)
                 (values stdout stderr)
                 (begin
                   (let ((msg (fmt #f "unexpected exit code (" exit-code ")")))
                    (info msg)
                    (fail msg))))))

  (define (run-ok . cmd-and-args)
    (run-with-exit-code zero? cmd-and-args))

  ;; Exit code 139 is a segfault, which is not acceptable
  (define (run-fail . cmd-and-args)
          (define (fails? x) (not
                               (or (= 139 x)
                                   (zero? x))))

    (run-with-exit-code fails? cmd-and-args)))


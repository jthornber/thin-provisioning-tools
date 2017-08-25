(library
  (process)

  (export run
          run-ok
          run-fail)

  (import (chezscheme)
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

  (define (fail msg)
    (raise (condition
             (make-error)
             (make-message-condition msg))))

  (define (build-command-line cmd-and-args)
    (apply fmt #f (map dsp (intersperse " " cmd-and-args))))

  (define (run . cmd-and-args)
    (with-temp-file (stdout-file stderr-file)
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
                   (info (fmt #f (dsp "stdout: ") stdout))
                   (info (fmt #f (dsp "stderr: ") stderr))
                   (fail (fmt #f (dsp "unexpected exit code (")
                              (num exit-code)
                              (dsp ")")))))))

  (define (run-ok . cmd-and-args)
    (run-with-exit-code zero? cmd-and-args))

  (define (run-fail . cmd-and-args)
    (define (not-zero? x) (not (zero? x)))

    (run-with-exit-code not-zero? cmd-and-args)))

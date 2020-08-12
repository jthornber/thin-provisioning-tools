(library
  (test-runner)
  (export run-tests)

  (import (rnrs)
          (only (chezscheme) load-shared-object)
          (fmt fmt)
          (list-utils)
          (functional-tests)
          (bcache bcache-tests)
          (cache-functional-tests)
          (era-functional-tests)
          (parser-combinators)
          (only (srfi s1 lists) break)
          (regex)
          (srfi s8 receive)
          (temp-file))

  ;;------------------------------------------------

  ;; Returns #t if the xs list matches prefix
  (define (begins-with prefix xs)
    (cond
      ((null? prefix) #t)
      ((null? xs) #f)
      ((eq? (car prefix) (car xs))
       (begins-with (cdr prefix) (cdr xs)))
      (else #f)))

  (define (split-list xs sep)
    (define (safe-cdr xs)
      (if (null? xs) '() (cdr xs)))

    (if (null? xs)
        '()
        (receive (p r) (break (lambda (c)
                                (eq? c sep))
                              xs)
                 (cons p (split-list (safe-cdr r) sep)))))

  (define (string->syms str sep)
    (map (lambda (cs)
           (string->symbol
             (list->string cs)))
         (split-list (string->list str) sep)))

  (define (mk-string-matcher pattern)
    (let ((prefix (string->syms pattern #\/)))
     (lambda (keys)
       (begins-with prefix keys))))

  (define (mk-regex-matcher pattern)
    (let ((rx (regex pattern)))
     (lambda (keys)
       (rx (apply string-append
                  (intersperse "/"
                               (map symbol->string keys)))))))

  (define (string-prefix? p str)
    (and (>= (string-length str) (string-length p))
         (string=? p (substring str 0 (string-length p)))))

  ;; If the filter begins with 're:' then we make a regex matcher, otherwise
  ;; we use a simple string matcher.
  (define (mk-single-matcher pattern)
    (if (string-prefix? "re:" pattern)
        (mk-regex-matcher (substring pattern 3 (string-length pattern)))
        (mk-string-matcher pattern)))

  (define (mk-filter patterns)
    (if (null? patterns)
        ; accept everything if no patterns
        (lambda (_) #t)

        ; Otherwise accept tests that pass a pattern
        (let ((filters (map mk-single-matcher patterns)))
         (fold-left (lambda (fn-a fn-b)
                      (lambda (keys)
                        (or (fn-a keys)
                            (fn-b keys))))
                    (car filters)
                    (cdr filters)))))

  (define (exec-help)
    (fmt (current-error-port)
         "Usage:" nl
         "  run-tests help" nl
         "  run-tests list <pattern>*" nl
         "  run-tests run [--disable-unlink] <pattern>*" nl nl
         (justify
           (string-append
             "Patterns are used to select tests.  There are two forms a pattern can take; "
             "either a literal such as 'cache-check/bad-option', or a regular expression (prefix with 're:')."))
         nl
         "eg," nl
         "    run-tests run cache-check/bad-option" nl
         "    run-tests run re:help" nl
         "    run-tests run cache-check thin-check re:.*missing.*file" nl
         ))

  (define (exec-run args)
    (let ((pred (mk-filter args)))
     (if (run-scenarios (filter pred (list-scenarios)))
         (exit)
         (exit #f))))

  (define (exec-list args)
    (let ((pred (mk-filter args)))
     (describe-scenarios (filter pred (list-scenarios)))))

  ;;------------------------------------------------
  ;; Command line parser

  (define whitespace
    (many+ (charset " \t\n")))

  (define (whitespace-delim ma)
    (>> (opt whitespace)
        (<* ma (opt whitespace))))

  (define (cmd-word str)
    (whitespace-delim (lit str)))

  (define (switch str)
    (whitespace-delim (>> (lit "--") (lit str))))

  (define not-switch
    (whitespace-delim
      (parse-m (<- c (neg-charset "- \t"))
               (<- cs (many* (neg-charset " \t")))
               (pure (list->string (cons c cs))))))

  (define (maybe ma)
    (alt (>> ma (pure #t))
         (pure #f)))

  (define help-command-line
    (>> (cmd-word "help") (pure exec-help)))

  (define run-command-line
    (parse-m
      (cmd-word "run")
      (<- dunlink (maybe (switch "disable-unlink")))
      (<- args (many* not-switch))
      (pure (lambda ()
              (if dunlink
                  (disable-unlink (exec-run args))
                  (exec-run args))))))

  (define list-command-line
    (parse-m
      (cmd-word "list")
      (<- args (many* not-switch))
      (pure (lambda () (exec-list args)))))

  (define command-line-parser
    (one-of help-command-line
            run-command-line
            list-command-line))

  (define (bad-command-line)
    (fmt (current-error-port) (dsp "bad command line\n"))
    (exec-help)
    (exit 1))

  ;; (<string>) -> thunk
  (define (parse-command-line)
    (let ((args (cdr (command-line))))
     (receive (v st)
              (parse command-line-parser
                     (apply string-append
                            (intersperse " " (cdr (command-line)))))
              (if (success? st)
                  v
                  bad-command-line))))

  ;;------------------------------------------------

  (define (run-tests)
    (with-dir "test-output"
      ((parse-command-line)))))

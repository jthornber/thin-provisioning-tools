(library
  (command-line thin-check)
  
  (export )
  
  (import (thin thin-check))

(define (help port)
 (fmt port
   "Usage: thin_check [options] {device|file}\n"
   "Options:\n"
   "  {-q|--quiet}\n"
   "  {-h|--help}\n"
   "  {-V|--version}\n"
   "  {--clear-needs-check-flag}\n"
   "  {--ignore-non-fatal-errors}\n"
   "  {--skip-mappings}\n"
   "  {--super-block-only}\n"))

(define whitespace
 (many+ (charset " \t\n")))

(define (whitespace-delim ma)
 (>> (opt whitespace)
  (<* ma (opt whitespace))))

(define (switch str)
 (whitespace-delim (>> (lit "--") (lit str))))

(define (switch-set . syms)
 (whitespace-delim
  (>> (lit "--")
   (apply one-of (map (lambda (s)
		       (>> (lit (symbol->string s)) (pure s)))
		  syms)))))

(define (short str)
 (whitespace-delim (>> (lit "-") (lit str))))

(define help-command-line
 (>> (one-of (switch "help") (switch "h"))
  (pure (lambda ()
	 (help (current-output-port))))))

;; FIXME: move somewhere
(define tools-version "0.8.0")

(define version-command-line
 (>> (one-of (short "V") (switch "version"))
  (pure (lambda ()
	 (display tools-version)
	 (display "\n")))))

(define switches
  (many*
   (switch-set
    'quiet
    'clear-needs-check-flag
    'ignore-non-fatal-errors
    'skip-mappings
    'super-block-only)))

(define not-switch
 (whitespace-delim
  (parse-m
   (<- c (neg-charset "- \t"))
   (<- cs (many* (neg-charset " \t")))
   (pure (list->string (cons c cs))))))

(define main-command-line
 (parse-m
  (<- ss switches)
  (<- path not-switch)
  eof
  (pure (lambda () (run ss path)))))

(define command-line-parser
 (one-of
    help-command-line
    version-command-line
    main-command-line))

(define (bad-command-line)
  (fmt (current-error-port) (dsp "bad command line\n"))
  (help (current-error-port))
  (exit 1))

(define (parse-command-line args)
  (receive (v st) (parse command-line-parser
		   (apply string-append (intersperse " " args)))
   ((if (success? st) v bad-command-line))))

)

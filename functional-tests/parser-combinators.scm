(library
  (parser-combinators)

  (export parse
          new-state
          get-state
          update-state
          parse-value
          error-m
          success?
          pure
          >>=
          alt
          peek
          >>
          lift
          lift2
          seq
          one-of
          opt
          opt-default
          many*
          many+
          if-m
          when-m
          unless-m
          <*
          times
          upto
          many-range
          surround
          getchar
          getchars
          accept-char
          charset
          neg-charset
          lit
          eof
          parse-m
          <-)

  (import (rnrs)
          (fmt fmt)
          (srfi s8 receive))

  ;;--------------------------------
  ;; Hand rolled state + parser monad
  ;;
  ;; The state must be immutable for this to work.  So we can't use hash
  ;; tables etc.

  ;;--------------------------------
  ;; Coordinates
  (define-record-type coordinate
                      (fields (immutable line coord-line)
                              (immutable char coord-char)))

  (define (new-coordinate)
    (make-coordinate 1 0))

  (define (inc-line c n)
    (make-coordinate (+ n (coord-line c)) 1))

  (define (inc-char c n)
    (make-coordinate (coord-line c) (+ n (coord-char c))))

  (define (coord-consume c str)
    (let loop ((newl (coord-line c))
               (newc (coord-char c))
               (input (string->list str)))
      (if (null? input)
          (make-coordinate newl newc)
          (let ((ch (car input)))
           (if (eq? #\newline ch)
               (loop (+ 1 newl) 0 (cdr input))
               (loop newl (+ 1 newc) (cdr input)))))))

  ;;--------------------------------
  ;; Parse context
  (define-record-type parse-state
                      (fields (immutable success-or-error st-soe)
                              (immutable coord st-coord)
                              (immutable input st-input)))

  (define (new-state input)
    (make-parse-state #t (new-coordinate) input))

  (define (st-update-soe st soe)
    (make-parse-state soe (st-coord st) (st-input st)))

  (define (st-update-coord st c)
    (make-parse-state (st-soe st) c (st-input st)))

  (define (st-update-input st in)
    (make-parse-state (st-soe st) (st-coord st) in))

  (define (st-consume st cs)
    (make-parse-state (st-soe st)
                      (st-coord st)
                      (let ((old (st-input st)))
                       (substring old (string-length cs) (string-length old)))))

  ;;--------------------------------
  ;; A vanilla state monad, carrying around a parse-state.
  ;;
  ;; st -> v, st

  ;; m a -> string -> v, state
  (define (parse ma input)
    (ma (new-state input)))

  ;; m st
  (define (get-state)
    (lambda (st)
      (values st st)))

  ;; (st -> st) -> m ()
  (define (update-state fn)
    (lambda (st)
      (values '() (fn st))))

  (define (parse-value ma input)
    (receive (v st) (parse ma input)
             v))

  (define (error-m fmt-doc)
    (update-state
      (lambda (st)
        (st-update-soe st (fmt #f fmt-doc)))))

  (define (success? st)
    (eq? (st-soe st) #t))

  ;; a -> m a
  (define (pure v)
    (lambda (st)
      (values v st)))

  ;; m a -> (a -> m b) -> m b
  (define (>>= ma fn)
    (lambda (st)
      (receive (v st2) (ma st)
               (if (success? st2)
                   ((fn v) st2)
                   (values v st2)))))

  ;; m a -> m a -> m a
  (define (alt ma1 ma2)
    (lambda (st)
      (receive (v st2) (ma1 st)
               (if (success? st2)
                   (values v st2)
                   (ma2 st)))))

  ;; m a -> m a (but doesn't modify state)
  (define (peek ma)
    (lambda (st)
      (receive (v st2) (ma st)
               (values v (st-update-soe st (st-soe st2))))))

  ;;--------------------------------
  ;; General monad combinators
  ;;
  ;; These only use fail_, pure, >>=, alt

  ;; m a -> m b -> m b
  (define (>> ma mb)
    (>>= ma (lambda (v) mb)))

  ;; (a -> b) -> m a -> m b
  (define (lift fn ma)
    (>>= ma (lambda (a)
              (pure (fn a)))))

  ;; (a -> b -> c) -> m a -> m b -> m c
  (define (lift2 fn ma mb)
    (>>= ma (lambda (a)
              (>>= mb (lambda (b)
                        (pure (fn a b)))))))

  ;; [m a] -> m [a]
  (define (seq . ms)
    (let loop ((ms ms))
     (if (null? ms)
         (pure '())
         (lift2 cons (car ms) (loop (cdr ms))))))

  ;; m a -> m a -> m a
  (define (one-of . xs)
    (let loop ((xs xs))
     (if (null? xs)
         (error-m (dsp "one-of found no match"))
         (alt (car xs) (loop (cdr xs))))))

  ;; m a -> m [a]
  (define (opt ma)
    (alt (lift list ma)
         (pure '())))

  ;; m a -> m b -> m (a|b)
  (define (opt-default ma default)
    (alt ma (pure default)))

  (define (mk-conser v)
    (lambda (vs) (cons v vs)))

  ;; m a -> m [a]
  (define (many* ma)
    (alt (>>= ma
              (lambda (v)
                (lift (mk-conser v)
                      (many* ma))))
         (pure '())))

  ;; FIXME: why doesn't this work? (blows stack)
  ;; (defun many* (ma)
  ;;   (alt (lift2 #'cons ma (many* ma))
  ;;        (pure nil)))

  (define (many+ ma)
    (lift2 cons ma (many* ma)))

  ;; Bool -> m a -> m b -> Either (m a) (m b)
  (define (if-m p ma mb)
    (if p ma mb))

  ;; Bool -> m a -> m a
  (define (when-m p ma)
    (if-m p ma (error-m (dsp "when-m failed"))))

  (define (unless-m p ma)
    (if-m p (error-m (dsp "unless-m failed")) ma))

  ;; m a -> m b -> m a
  (define (<* ma mb)
    (>>= ma (lambda (a)
              (>> mb (pure a)))))

  ;;--------------------------------
  ;; Combinators that use parse-m

  (define (times n ma)
    (if (zero? n)
        (pure '())
        (lift2 cons ma (times (- n 1) ma))))

  (define (upto max ma)
    (if (zero? max)
        (pure '())
        (>>= (opt ma)
             (lambda (a)
               (if (null? a)
                   (pure '())
                   (lift (mk-conser (car a))
                         (upto (- max 1) ma)))))))

  (define (many-range min max ma)
    (>>= (times min ma)
         (lambda (vs)
           (>>= (upto (- max min) ma)
                (lambda (os)
                  (pure (append vs os)))))))

  ;; ma -> mb -> mb
  (define (surround ma mb)
    (>> ma (<* mb ma)))

  ;;--------------------------------
  ;; Basic combinators
  ;;
  ;; We should try and keep these to a minimum.  These are allowed to
  ;; know the internals of the monad.
  (define getchar
    (lambda (st)
      (let ((input (st-input st)))
        (if (zero? (string-length input))
            (values #f (st-update-soe st "end of input for getchar"))
            (let ((c (string-ref input 0)))
             (values c (st-consume st (list->string (list c)))))))))

  (define (getchars n)
    (lambda (st)
      (let ((input (st-input st)))
       (if (>= (string-length input) n)
           (let ((cs (substring input 0 n)))
            (values cs (st-consume st cs)))
           (values '() (st-update-soe st "insufficient input for getchars"))))))

  ;;--------------------------------
  ;; Higher order combinators
  ;;
  ;; These should just use other combinators

  (define (accept-char pred)
    (>>= getchar
         (lambda (c)
           (when-m (pred c)
                   (pure c)))))

  ;; FIXME: slow
  (define (charset str)
    (let ((cs (string->list str)))
     (accept-char (lambda (c)
                    (member c cs)))))

  (define (neg-charset str)
    (let ((cs (string->list str)))
     (accept-char (lambda (c)
                    (not (member c cs))))))

  (define (lit tok)
    (let ((len (string-length tok)))
     (>>= (getchars len)
          (lambda (str)
            (when-m (string=? tok str)
                    (pure tok))))))

  #|
  (defun token (str &optional sym)
    (unless sym
      (setf sym (symb-keyword str)))
    (>> (lit str) (pure sym)))
  |#

  (define (eof s)
    (if (zero? (string-length s))
        (values #t #t "")
        (error-m (dsp "eof expected no input (but there was)"))))

  ;;----------------------------------------------------------------
  ;; Imperative notation for when the combinators are cumbersome

  (define-syntax parse-m
    (syntax-rules (<-)
      ((_ (<- v ma) clauses ...)
       (>>= ma
            (lambda (v)
              (parse-m clauses ...))))

      ((_ ma) ma)

      ((_ ma clauses ...)
       (>> ma (parse-m clauses ...)))))

  (define-syntax <-
    (lambda (x)
      (syntax-violation '<- "misplaced auxilliary keyword" x)))
  )

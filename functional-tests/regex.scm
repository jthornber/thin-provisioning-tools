(library
  (regex)
  (export regex)
  (import (chezscheme)
          (fmt fmt)
          (loops)
          (prefix (parser-combinators) p:)
          (srfi s8 receive)
          (matchable))

  ;; Simple regex library, because it's friday and I'm bored.
  ;; Playing with the ideas in: https://swtch.com/~rsc/regexp/regexp2.html
  ;; which reminded me of reading through the source code to Sam in '93.

  ;; Rather than parsing a string we'll use expressions.
  ;; (lit <string>)
  ;; (seq rx1 rx2)
  ;; (alt rx1 rx2)
  ;; (opt rx)
  ;; (star rx)
  ;; (plus rx)
  ;;
  ;; The expressions get compiled into a vector of vm instructions.
  ;; (char pred) ; where fn :: char -> bool
  ;; (match)
  ;; (jmp x)
  ;; (split x y)

  (define (append-instr code . i) (append code i))
  (define (label-instr l) `(label ,l))
  (define (jmp-instr l) `(jmp ,l))
  (define (char-instr fn) `(char ,fn))
  (define (split-instr l1 l2) `(split ,l1 ,l2))
  (define (match-instr) '(match))
  (define (match-instr? instr) (equal? '(match) instr))

  (define (label-code label code)
    (cons (label-instr label) code))

  ;; Compiles to a list of labelled instructions that can later be flattened
  ;; into a linear sequence.
  (define (lit str)
    (map (lambda (c1)
           (char-instr
             (lambda (c2)
               (char=? c1 c2))))
         (string->list str)))

  (define (seq rx1 rx2)
    (append rx1 rx2))

  (define (alt rx1 rx2)
    (let ((label1 (gensym))
          (label2 (gensym))
          (tail (gensym)))
      (let ((c1 (label-code label1
                            (append-instr rx1 (jmp-instr tail))))
            (c2 (label-code label2 rx2)))
        (cons (split-instr label1 label2)
              (append-instr (append c1 c2) (label-instr tail))))))

  (define (opt rx)
    (let ((head (gensym))
          (tail (gensym)))
      (cons (split-instr head tail)
            (label-code head
                        (append-instr rx (label-instr tail))))))

  (define (star rx)
    (let ((head (gensym))
          (body (gensym))
          (tail (gensym)))
      (label-code head
                  (cons (split-instr body tail)
                        (label-code body
                                    (append-instr rx
                                                  (jmp-instr head)
                                                  (label-instr tail)))))))

  (define (plus rx)
    (let ((head (gensym))
          (tail (gensym)))
      (label-code head
                  (append-instr rx
                                (split-instr head tail)
                                (label-instr tail)))))

  (define (label-locations code)
    (let ((locs (make-eq-hashtable)))
     (let loop ((pc 0)
                (code code))
       (if (null? code)
           locs
           (match (car code)
                  (('label l)
                   (begin
                     (hashtable-set! locs l pc)
                     (loop pc (cdr code))))
                  (instr
                    (loop (+ 1 pc) (cdr code))))))))

  (define (remove-labels code locs)
    (let loop ((pc 0)
               (code code)
               (acc '()))
      (if (null? code)
          (reverse acc)
          (match (car code)
                 (('label l)
                  (loop pc (cdr code) acc))

                 (('jmp l)
                  (loop (+ 1 pc) (cdr code)
                        (cons `(jmp ,(hashtable-ref locs l #f)) acc)))

                 (('split l1 l2)
                  (loop (+ 1 pc) (cdr code)
                        (cons `(split ,(hashtable-ref locs l1 #f)
                                      ,(hashtable-ref locs l2 #f))
                              acc)))

                 (instr (loop (+ 1 pc) (cdr code) (cons instr acc)))))))

  (define (optimise-jumps! code)
    (define (single-pass)
      (let ((changed #f))
       (upto (n (vector-length code))
             (match (vector-ref code n)
                    (('jmp l)
                     (when (match-instr? (vector-ref code l))
                       (set! changed #t)
                       (vector-set! code n (match-instr))))

                    (('split l1 l2)
                     (when (or (match-instr? (vector-ref code l1))
                               (match-instr? (vector-ref code l2)))
                       (set! changed #t)
                       (vector-set! code n (match-instr))))

                    (_ _)))
       changed))

    (let loop ()
     (when (single-pass)
       (loop)))
    code)

  (define (compile-to-symbols rx)
    (let ((rx (append-instr rx (match-instr))))
     (optimise-jumps!
       (list->vector
         (remove-labels rx (label-locations rx))))))

  ;; A 'thread' consists of an index into the instructions.  A 'yarn holds the
  ;; current threads.  Note there cannot be more threads than instructions, so
  ;; a yarn is represented as a vector the same length as the instructions.
  ;; Threads are run in lock step, all taking the same input.
  (define-record-type yarn
                      (fields (mutable size)
                              (mutable stack)
                              (mutable seen)))

  (define (mk-yarn count)
    (make-yarn 0 (make-vector count) (make-vector count #f)))

  (define (clear-yarn! y)
    (yarn-size-set! y 0)
    (vector-fill! (yarn-seen y) #f))

  (define (add-thread! y i)
    (unless (vector-ref (yarn-seen y) i)
      (vector-set! (yarn-seen y) i #t)
      (vector-set! (yarn-stack y) (yarn-size y) i)
      (yarn-size-set! y (+ 1 (yarn-size y)))))

  (define (pop-thread! y)
    (if (zero? (yarn-size y))
        #f
        (begin
          (yarn-size-set! y (- (yarn-size y) 1))
          (vector-ref (yarn-stack y) (yarn-size y)))))

  (define (no-threads? y)
    (zero? (yarn-size y)))

  (define-syntax swap
    (syntax-rules ()
      ((_ x y)
       (let ((tmp x))
        (set! x y)
        (set! y tmp)))))

  ;; FIXME: hack
  (define end-of-string #\x0)

  (define (compile-rx rx)
    (let* ((sym-code (compile-to-symbols rx))
           (code-len (vector-length sym-code))
           (threads (mk-yarn code-len))
           (next-threads (mk-yarn code-len))
           (code #f))

      (define (compile-instr instr)
        (match instr
               (('match)
                (lambda (in-c pc) 'match))

               (('char fn)
                (lambda (in-c pc)
                  ;; use eq? because in-c isn't always a char
                  (when (fn in-c)
                    (add-thread! next-threads (+ 1 pc)))))

               (('jmp l)
                (lambda (in-c pc)
                  (add-thread! threads l)))

               (('split l1 l2)
                (lambda (in-c pc)
                  (add-thread! threads l1)
                  (add-thread! threads l2)))))

      (define (step in-c)
        (let loop ((pc (pop-thread! threads)))
         (and pc
              (if (eq? 'match ((vector-ref code pc) in-c pc))
                  'match
                  (loop (pop-thread! threads))))))

      ;(fmt #t (dsp "running ") (pretty code) nl)

      ;; compile to closures to avoid calling match in the loop.
      (upto (n code-len)
            (set! code (vector-map compile-instr sym-code)))

      (lambda (txt)
        (add-thread! threads 0)
        (let ((txt-len (string-length txt)))
         (let c-loop ((c-index 0))
          (if (< c-index txt-len)
              ;; FIXME: make step return a bool
              (if (eq? 'match (step (string-ref txt c-index)))
                  #t
                  (if (no-threads? next-threads)
                      #f
                      (begin
                        (swap threads next-threads)
                        (clear-yarn! next-threads)
                        (c-loop (+ 1 c-index)))))
              (eq? 'match (step end-of-string))))))))

  ;;;--------------------------------------------------------
  ;;; Parser

  ;; FIXME: ^ and ? aren't in the grammar, and eos/$ isn't wired up

  (define raw-char
    (let ((meta-chars (string->list "\\^$*+?[]()|")))
     (define (not-meta c)
       (not (member c meta-chars)))

     (p:alt (p:parse-m (p:<- c (p:accept-char not-meta))
                       (p:pure c))
            (p:>> (p:lit "\\")
                  (p:accept-char (lambda (c) #t))))))

  (define (bracket before after ma)
    (p:>> before (p:<* ma after)))

  (define (negate fn)
    (lambda (c)
      (not (fn c))))

  ;;-----------------------------------------------------------
  ;; Low level char combinators.  These build char predicates.

  ;; char-rx := any non metacharacter | "\" metacharacter
  ;; builds a predicate that accepts the char
  (define char-rx
    (p:parse-m (p:<- c1 raw-char)
               (p:pure (lambda (c2)
                         (char=? c1 c2)))))

  ;; range := char-rx "-" char-rx
  (define range
    (p:parse-m (p:<- c1 raw-char)
               (p:lit "-")
               (p:<- c2 raw-char)
               (p:pure (lambda (c)
                         (char<=? c1 c c2)))))

  ;; set-items := range | char-rx
  (define set-item (p:alt range char-rx))

  (define (or-preds preds)
    (lambda (c)
      (let loop ((preds preds))
       (if (null? preds)
           #f
           (or ((car preds) c)
               (loop (cdr preds)))))))

  ;; set-items := set-item+
  (define set-items
    (p:lift or-preds (p:many+ set-item)))

  ;; negative-set := "[^" set-items "]"
  (define negative-set
    (bracket (p:lit "[^")
             (p:lit "]")
             (p:lift negate set-items)))

  ;; positive-set := "[" set-items "]"
  (define positive-set
    (bracket (p:lit "[")
             (p:lit "]")
             set-items))

  ;; set := positive-set | negative-set
  (define set (p:alt positive-set negative-set))

  ;; eos := "$"
  ;; FIXME: ???
  (define eos (p:lit "$"))

  ;; any := "."
  (define any (p:>> (p:lit ".") (p:pure (lambda (_) #t))))

  ;;-----------------------------------------------------------
  ;; Higher level combinators, these build a symbolic rx

  ;; The definitions start being mutually recursive from here on in, so we make
  ;; them thunks to defer evaluation.

  ;; group := "(" rx ")"
  (define (group)
    (fmt #t (dsp "group") nl)
    (bracket (p:lit "(")
             (p:lit ")")
             (rx)))

  ;; elementary-rx := group | any | eos | char-rx | set
  ;; FIXME: put eos and group back in
  (define (elementary-rx)
    (p:lift (lambda (fn)
              (list (char-instr fn)))
            (p:one-of any char-rx set)))

  ;; plus-rx := elementary-rx "+"
  (define (plus-rx)
    (p:lift plus (p:<* (elementary-rx) (p:lit "+"))))

  ;; star-rx := elementary-rx "*"
  (define (star-rx)
    (p:lift star (p:<* (elementary-rx) (p:lit "*"))))

  ;; basic-rx := star-rx | plus-rx | elementary-rx
  (define (basic-rx)
    (p:one-of (star-rx) (plus-rx) (elementary-rx)))

  ;; simple-rx := basic-rx+
  (define (simple-rx)
    (define (combine rs)
      (fold-left seq (car rs) (cdr rs)))

    (p:lift combine (p:many+ (basic-rx))))

  ;; rx := simple-rx ("|" simple-rx)*
  (define (rx)
    (define (combine rs)
      (fold-left alt (car rs) (cdr rs)))

    (p:parse-m (p:<- r (simple-rx))
               (p:<- rest (p:many* (p:>> (p:lit "|")
                                         (simple-rx))))
               (p:pure (combine (cons r rest)))))

  ;;-----------------------------------------------------------------------
  ;; The top level routine, parses the regex string and compiles it into a
  ;; matcher, or returns false if the parse failed.
  ;; regex :: string -> (matcher <string>)
  (define (regex str)
    (receive (v st) (p:parse (rx) str)
             (if (p:success? st)
                 (compile-rx v)
                 #f))))

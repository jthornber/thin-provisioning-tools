(library
  (xml)

  (export tag
          vcat)

  (import (rnrs)
          (fmt fmt)
          (list-utils))

  (define (dquote doc)
    (cat (dsp #\") doc (dsp #\")))

  (define (to-attribute-name sym)
    (define (to-underscore c)
      (if (eq? #\- c) #\_ c))

    (list->string (map to-underscore (string->list (symbol->string sym)))))

  (define (attribute dotted-pair)
    (let ((key (to-attribute-name (car dotted-pair)))
          (val (cdr dotted-pair)))
      (cat (dsp key)
           (dsp "=")
           (dquote ((if (string? val) dsp wrt) val)))))

  (define (%open-tag sym attrs end)
    (cat (dsp "<")
         (dsp sym)
         (dsp " ")
         (apply cat (intersperse (dsp " ")
                                 (map attribute attrs)))
         (dsp end)))

  (define (open-tag sym attrs)
    (%open-tag sym attrs ">"))

  (define (simple-tag sym attrs)
    (%open-tag sym attrs "/>"))

  (define (close-tag sym)
    (cat (dsp "</")
         (dsp sym)
         (dsp ">")))

  (define (tag sym attrs . body)
    (if (null? body)
        (simple-tag sym attrs)
        (begin
          (cat (open-tag sym attrs)
               nl
               (apply cat body)
               nl
               (close-tag sym)))))

  (define (vcat docs)
    (apply cat (intersperse nl docs))))

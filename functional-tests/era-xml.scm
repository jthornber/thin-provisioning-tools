(library
  (era-xml)
  (export generate-xml)
  (import (rnrs)
          (list-utils)
          (fmt fmt)
          (generators)
          (xml)
          (srfi s27 random-bits)
          (only (srfi s1 lists) iota))

  (define (rand-percent pc)
    (if (< (random-integer 100) pc)
        "true"
        "false"))

  (define (generate-writeset era nr-bits)
    (tag 'writeset `((era . ,era)
                     (nr-bits . , nr-bits))
         (vcat
           (map (lambda (bit)
                  (tag 'bit `((block . ,bit)
                              (value . ,(rand-percent 10)))))
                (iota nr-bits)))))

  (define (generate-xml block-size nr-blocks current-era nr-writesets)
    (tag 'superblock `((uuid . "")
                       (block-size . ,block-size)
                       (nr-blocks . ,nr-blocks)
                       (current-era . ,current-era))
           (cat
             (vcat
               (map (lambda (ws)
                      (generate-writeset (- current-era ws) nr-blocks))
                    (iota nr-writesets)))
             nl
             (tag 'era-array '()
                  (vcat
                    (map (lambda (n)
                           (tag 'era `((block . ,n)
                                       (era . ,(random-integer current-era)))))
                         (iota nr-blocks)))))))
  )

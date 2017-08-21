(import (chezscheme)
        (functional-tests)
        (cache-functional-tests)
        (thin-functional-tests))

(register-thin-tests)
(register-cache-tests)

(if (run-scenarios (list-scenarios))
    (exit)
    (exit #f))


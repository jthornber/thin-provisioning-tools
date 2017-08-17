(import (chezscheme)
        (functional-tests)
        (cache-functional-tests)
        (thin-functional-tests))

(register-thin-tests)
(register-cache-tests)
(run-scenarios (list-scenarios))


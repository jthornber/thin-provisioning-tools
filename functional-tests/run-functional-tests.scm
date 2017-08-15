(import (chezscheme)
        (functional-tests)
        (thin-functional-tests))

(register-thin-tests)
(run-scenarios (list-scenarios))


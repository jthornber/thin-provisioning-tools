Feature: thin_delta
  Scenario: print version (-V flag)
    When I run `thin_delta -V`
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run `thin_delta --version`
    Then it should pass with version

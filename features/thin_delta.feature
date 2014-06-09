Feature: thin_delta
  Scenario: print version (-V flag)
    When I run `thin_delta -V`
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run `thin_delta --version`
    Then it should pass with version

  Scenario: print help
    When I run `thin_delta --help`
    Then it should pass with:

    """
    Usage: thin_delta [options]
    Options:
      {-h|--help}
      {-V|--version}
    """

  Scenario: print help
    When I run `thin_delta -h`
    Then it should pass with:
    """
    Usage: thin_delta [options]
    Options:
      {-h|--help}
      {-V|--version}
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_delta --unleash-the-hedeghogs`
    Then it should fail

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
    Usage: thin_delta [options] --snap1 <snap> --snap2 <snap> <device or file>
    Options:
      {--verbose}
      {-h|--help}
      {-V|--version}
    """

  Scenario: print help
    When I run `thin_delta -h`
    Then it should pass with:
    """
    Usage: thin_delta [options] --snap1 <snap> --snap2 <snap> <device or file>
    Options:
      {--verbose}
      {-h|--help}
      {-V|--version}
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_delta --unleash-the-hedeghogs`
    Then it should fail

  Scenario: --snap1 must be specified
    When I run `thin_delta --snap2 45 foo`
    Then it should fail with:
    """
    --snap1 not specified.
    """

  Scenario: --snap2 must be specified
    When I run `thin_delta --snap1 45 foo`
    Then it should fail with:
    """
    --snap2 not specified.
    """

  Scenario: device must be specified
    When I run `thin_delta --snap1 45 --snap2 50`
    Then it should fail with:
    """
    No input device provided.
    """

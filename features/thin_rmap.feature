Feature: thin_rmap
  Scenario: print version (-V flag)
    When I run `thin_rmap -V`
    Then it should pass with:

    """
    0.1.5
    """

  Scenario: print version (--version flag)
    When I run `thin_rmap --version`
    Then it should pass with:

    """
    0.1.5
    """

  Scenario: print help
    When I run `thin_rmap --help`
    Then it should pass with:

    """
    Usage: thin_rmap [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
      {--region <block range>}*
    Where:
      <block range> is of the form <begin>..<one-past-the-end>
      for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45
    """

  Scenario: print help
    When I run `thin_rmap -h`
    Then it should pass with:
    """
    Usage: thin_rmap [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
      {--region <block range>}*
    Where:
      <block range> is of the form <begin>..<one-past-the-end>
      for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45
    """

  Scenario: Unrecognised option should cause failure
    When I run `thin_rmap --unleash-the-hedeghogs`
    Then it should fail

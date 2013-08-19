Feature: cache_dump
  Scenario: print version (-V flag)
    When I run cache_dump with -V
    Then it should pass with version

  Scenario: print version (--version flag)
    When I run cache_dump with --version
    Then it should pass with version

  Scenario: print help (-h)
    When I run cache_dump with -h
    Then it should pass with:

    """
    Usage: cache_dump [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
    """

  Scenario: print help (--help)
    When I run cache_dump with -h
    Then it should pass with:

    """
    Usage: cache_dump [options] {device|file}
    Options:
      {-h|--help}
      {-V|--version}
    """

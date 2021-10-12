"""
The stages (e.g. assembly, training, scoring but also sub-tasks like reporting) have
various dependencies between each other, and on integrations, that are necessary even
for certain kinds of unit-tests in addition to integrations tests and functional tests.
Although the tests have been consolidated to one script, with package scoping, they can
later be broken apart if we want to do that.
"""

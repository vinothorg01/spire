# Spire Release Process

_(example for a v3.0.0 release)_

1. Create branch if it doesn't exist; `release/3.x` (release branches should only be created at major versions).
    1. `$ git checkout main`
    2. `$ git pull --rebase`
    3. `$ git checkout -b release/3.x`
2. Update `/CHANGELOG.md` (skip older versions).
3. Add any relevant documentation into `/docs` as .md files (e.g., architecture, RFCs, guides).
4. Update version string within `/spire/version.py`.
5. Commit the changelog, version.py, and docs on the release branch (`release/3.x` in this case)
    1. `$ git add .`
    2. `$ git commit -m "Version 3.0.0 (Stable)"`
6. Create an annotated tag `v3.0.0` (tags should always be off the release branch; `release/3.x` in this case).
    1. `$ git tag -a v3.0.0 -m "Version 3.0.0 (Stable)"`
    2. `$ git push --follow-tags`
7. Create GitHub release from tag; please include the changelog content along with any docs links.
8. Add a built wheel to release (please use wheel's instead of eggs).
    1. `$ python setup.py sdist bdist_wheel` (where `python` should be our production Python distribution)


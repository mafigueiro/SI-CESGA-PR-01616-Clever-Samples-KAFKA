# SI-CESGA-PR-01616-Clever-Samples-KAFKA README


Service to import samples data into clever via streaming.

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Flake8 Status](./reports/badges/flake8-badge.svg)](./reports/flake8/index.html)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![Tests Status](./reports/badges/junit-tests-badge.svg)](./reports/junit/html-test-report.html)
![Tests Coverage](./reports/badges/cov-badge.svg)

---

## Version

Current version is 1.0.0 and was set according to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Project's version should be updated, when applicable:

- In this very file.
- In the changelog.
- In the pyproject.toml file.

## Prerequisites

- [uv](https://github.com/astral-sh/uv)

## Installation

In order to install the project, simply run:

```bash
poetry install
```

## Building

In order to generate a .whl file to distribute the project, simply run:

```bash
poetry build
```

## Testing

```bash
pytest tests                                           # Will run all tests
pytest tests/test_module.py                            # Will run test module
pytest tests/test_module.py::test_function             # Will run test function inside test module
pytest tests/test_module.py::TestClass::test_method    # Will run test method inside test class
```

## License

Developed by Centro Tecnolóxico de Telecomunicacións de Galicia (GRADIANT) (c) 2025

See license file for more information.

## Authors

- [Marco Alvarezx](mafigueiro@gradiant.org)

## Maintainer

- [Marco Alvarezx](mafigueiro@gradiant.org)

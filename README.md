# asset_base

A package for obtaining, managing and provision of financial securities meta-data and time-series data.

## Installation

```bash
$ pip install asset_base
```

## Usage

  - The package is at an early stage of development.
  - There may still be some bugs.
  - There is a good test suite which was used to develop the package. It is therefore a development test suite, not a use qualification test suite centred around the user. That still needs to be done. Code coverage analysis also needs to be done.
  - The package is fairly well documented in the docstrings but still need a proper Sphinx document body.

### Identifier semantics in time-series workflows

- The `TimeSeriesProcessor` input/output column name is `identity`.
- In current `Asset.get_time_series_processor()` flows, this column stores the `Asset` instance itself (not just `Asset.identity_code` string values).
- This allows downstream workflows to access full asset attributes directly from grouped/pivoted data.

### Identity code initialization

- `identity_code` is a stored database column, not a property computed on every access.
- Classes using `IdentityCodeMixin` must only call `sync_identity_code()` after assigning every attribute used by `_get_identity_code()`.
- Simple classes can declare `IDENTITY_CODE_FIELD` and let the mixin copy that attribute into `identity_code`.
- Derived-code classes such as `Issuer` and `Forex` should continue to override `_get_identity_code()` explicitly.
- Avoid delaying identity-code population to ORM insert hooks because some constructors need `identity_code` immediately during object setup.

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

My development time on this package is quite limited by my other duties as an engineer and a quantitative portfolio manager so your contributions will be most welcome.

## License

`asset_base` was created by Justin Solms. It is licensed under the terms of the MIT license.

## Credits

The package has been motivate by and developed for internal use at Index Solutions. Index Solutions is a “trading as” name of Sunstrike Capital (Proprietary) Limited, an authorised Financial Services Provider (license number 44691) and a registered South African company (2011/004440/07).

`asset_base` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).

## VS Code Python Environment Consistency

This workspace is configured to use a single Python selector and consistent environment behavior for running code, terminal sessions, debugging, and tests.

### Sync across machines

- Commit and push `.vscode/settings.json` so these workspace settings travel with the repo.
- On each machine, install/enable both VS Code extensions:
  - `ms-python.python`
  - `ms-python.vscode-python-envs`
- Ensure the target conda environment exists locally (for example, `dev`).
- After pulling changes on a new machine, run `Developer: Reload Window` once.
- Open a new terminal and confirm the interpreter via `Python: Select Interpreter`.

### Notes

- VS Code Settings Sync does not reliably propagate workspace settings unless the workspace files are shared.
- Notebook kernels can still be selected independently from the Python interpreter.

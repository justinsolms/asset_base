import pandas as pd
import pytest
import requests
from click.testing import CliRunner

from notebooks import check_eod_data

get_distributions_dict = check_eod_data.get_distributions_dict


@pytest.fixture(autouse=True)
def distribution_metadata(monkeypatch):
    monkeypatch.setattr(
        check_eod_data, "get_distributions_dict",
        lambda: {ticker: True for ticker in check_eod_data.get_etf_tickers()},
    )


def test_summary_counts_and_progress(monkeypatch, capsys):
    eod = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "open": [0, 0, 1],
        "volume": [0, 1, 1],
        "close": [1, 100, 1],
    })
    dividends = pd.DataFrame({
        "date": ["2024-01-01", "2024-02-01"],
        "value": [1, 100],
        "unadjustedValue": [0, 0],
    })

    def fetch(ticker, endpoint):
        if ticker == "FIRST.JSE":
            return {"eod": eod, "div": dividends, "splits": pd.DataFrame()}[endpoint]
        return pd.DataFrame()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE", "SECOND.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    check_eod_data.main()
    captured = capsys.readouterr()
    summary = captured.out.split("SUMMARY REPORT", 1)[1]
    first = next(line for line in summary.splitlines() if line.startswith("FIRST.JSE"))
    second = next(line for line in summary.splitlines() if line.startswith("SECOND.JSE"))
    assert "ZEROS(EOD: 3, DIV: 2)" in first
    assert "SCALING(EOD: 2, DIV: 1)" in first
    assert "NO DATA(SPLIT)" in first
    assert "NO DATA(EOD, DIV, SPLIT)" in second
    assert "INCOMPLETE" in second
    assert "CLEAN" not in second
    assert "Total Zero Value Issues: 5" in summary
    assert "Zero Value Issues by Data Type: EOD: 3, DIV: 2" in summary
    assert "Total Scaling Issues: 3" in summary
    assert "Scaling Issues by Data Type: EOD: 2, DIV: 1" in summary
    assert "2/2" in captured.err
    assert "Date:" not in captured.out


@pytest.mark.parametrize("test_mode", [False, True])
def test_summary_is_alphabetical_regardless_of_audit_order(monkeypatch, test_mode):
    tickers = ["ZED.JSE", "ALPHA.JSE", "MID.JSE"]
    calls = []

    def fetch(ticker, endpoint):
        calls.append(ticker)
        return pd.DataFrame()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: tickers)
    monkeypatch.setattr(check_eod_data.random, "shuffle", lambda values: values.reverse())
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    args = ["--no-div", "--no-splits"]
    if test_mode:
        args.append("--test")
    result = CliRunner().invoke(check_eod_data.cli, args)
    assert result.exit_code == 0, result.output
    assert calls == (list(reversed(tickers)) if test_mode else tickers)
    summary = result.stdout.split("SUMMARY REPORT", 1)[1]
    rows = [line.split()[0] for line in summary.splitlines()
            if line.split() and line.split()[0] in tickers]
    assert rows == ["ALPHA.JSE", "MID.JSE", "ZED.JSE"]


def test_request_failure_is_not_reported_as_missing_or_clean(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise requests.exceptions.ConnectionError("sensitive request details")

    monkeypatch.setattr(check_eod_data.requests, "get", fail)
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    check_eod_data.main()
    output = capsys.readouterr().out
    summary = output.split("SUMMARY REPORT", 1)[1]
    assert "FETCH FAILED(EOD, DIV, SPLIT)" in summary
    assert "INCOMPLETE" in summary
    assert "NO DATA" not in summary
    assert "sensitive request details" not in output


@pytest.mark.parametrize(
    ("flags", "expected", "skipped"),
    [
        ([], ["eod", "div", "splits"], None),
        (["--no-eod"], ["div", "splits"], "SKIPPED(EOD)"),
        (["--no-div"], ["eod", "splits"], "SKIPPED(DIV)"),
        (["--no-splits"], ["eod", "div"], "SKIPPED(SPLIT)"),
        (["--no-eod", "--no-splits"], ["div"], "SKIPPED(EOD, SPLIT)"),
    ],
)
def test_cli_selects_endpoints(monkeypatch, flags, expected, skipped):
    calls = []

    def fetch(ticker, endpoint):
        calls.append((ticker, endpoint))
        return pd.DataFrame()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(check_eod_data.cli, flags)
    assert result.exit_code == 0, result.output
    assert calls == [("FIRST.JSE", endpoint) for endpoint in expected]
    assert "SUMMARY REPORT" in result.output
    if skipped:
        assert skipped in result.output


def test_cli_verbosity_preserves_summary(monkeypatch):
    def fetch(ticker, endpoint):
        if endpoint == "eod":
            return pd.DataFrame({
                "date": ["2024-01-01", "2024-01-02"],
                "volume": [0, 0],
                "close": [1, 100],
            })
        return pd.DataFrame()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    runner = CliRunner()
    quiet = runner.invoke(check_eod_data.cli)
    verbose = runner.invoke(check_eod_data.cli, ["--verb", "1"])
    assert quiet.exit_code == verbose.exit_code == 0
    assert "Date:" not in quiet.output
    assert "Date: 2024-01-01 | Value: 0" in verbose.output
    assert "Date: 2024-01-02 | x100 Jump" in verbose.output
    assert quiet.stdout.split("SUMMARY REPORT", 1)[1] == verbose.stdout.split("SUMMARY REPORT", 1)[1]
    assert "ZEROS(EOD: 2)" in quiet.stdout
    assert "SCALING(EOD: 1)" in quiet.stdout


def test_zero_categories_in_summary_and_verbose_output(monkeypatch):
    data = {
        "eod": pd.DataFrame({"date": ["2024-01-01"], "open": [0]}),
        "div": pd.DataFrame({"date": ["2024-01-02"], "value": [0]}),
        "splits": pd.DataFrame({"date": ["2024-01-03"], "split": [0]}),
    }

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE", "SECOND.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", lambda ticker, endpoint: data[endpoint].copy())
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(check_eod_data.cli, ["--verb", "1"])
    assert result.exit_code == 0, result.output
    for data_type in ("EOD", "DIVIDENDS", "SPLITS"):
        assert f"ZERO VALUE found in FIRST.JSE [{data_type}]" in result.output
    summary = result.stdout.split("SUMMARY REPORT", 1)[1]
    for ticker in ("FIRST.JSE", "SECOND.JSE"):
        line = next(line for line in summary.splitlines() if line.startswith(ticker))
        assert "ZEROS(EOD: 1, DIV: 1, SPLIT: 1)" in line
    assert "Total Zero Value Issues: 6" in summary
    assert "Zero Value Issues by Data Type: EOD: 2, DIV: 2, SPLIT: 2" in summary
    assert "Total Scaling Issues: 0" in summary
    assert "Scaling Issues by Data Type:" not in summary


def test_scaling_categories_across_tickers_and_verbosity(monkeypatch):
    data = {
        "eod": pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "close": [1, 100]}),
        "div": pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "value": [100, 1]}),
        "splits": pd.DataFrame(),
    }
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE", "SECOND.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", lambda ticker, endpoint: data[endpoint].copy())
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    runner = CliRunner()
    quiet = runner.invoke(check_eod_data.cli)
    verbose = runner.invoke(check_eod_data.cli, ["--verb", "1"])
    assert quiet.exit_code == verbose.exit_code == 0
    summary = quiet.stdout.split("SUMMARY REPORT", 1)[1]
    assert summary == verbose.stdout.split("SUMMARY REPORT", 1)[1]
    for ticker in ("FIRST.JSE", "SECOND.JSE"):
        line = next(line for line in summary.splitlines() if line.startswith(ticker))
        assert "SCALING(EOD: 1, DIV: 1)" in line
    assert "Total Scaling Issues: 4" in summary
    assert "Scaling Issues by Data Type: EOD: 2, DIV: 2" in summary
    assert "SCALING ISSUE suspected in FIRST.JSE [EOD]" in verbose.output
    assert "SCALING ISSUE suspected in FIRST.JSE [DIVIDENDS]" in verbose.output


@pytest.mark.parametrize(
    ("flags", "expected_zeros", "skipped"),
    [
        ([], "ZEROS(EOD: 2, DIV: 1, SPLIT: 1)", []),
        (["--no-volume"], "ZEROS(EOD: 1, DIV: 1, SPLIT: 1)", ["EOD volume"]),
        (["--no-zeros"], None, ["zero-value checks"]),
        (["--no-volume", "--no-zeros"], None, ["zero-value checks", "EOD volume"]),
    ],
)
def test_zero_and_volume_options(monkeypatch, flags, expected_zeros, skipped):
    data = {
        "eod": pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "open": [0, 1], "volume": [0, 10], "close": [1, 100],
        }),
        "div": pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "value": [1, 100], "unadjustedValue": [0, 1],
        }),
        "splits": pd.DataFrame({"date": ["2024-01-01"], "split": [0]}),
    }
    calls = []

    def fetch(ticker, endpoint):
        calls.append(endpoint)
        return data[endpoint].copy()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)
    result = CliRunner().invoke(check_eod_data.cli, [*flags, "--verb", "1"])

    assert result.exit_code == 0, result.output
    assert calls == ["eod", "div", "splits"]
    assert "SCALING(EOD: 1, DIV: 1)" in result.stdout
    for label in skipped:
        assert label in result.stdout
    if expected_zeros is None:
        assert "ZEROS(" not in result.stdout
        assert "ZERO VALUE found" not in result.output
        assert "Total Zero Value Issues: SKIPPED" in result.stdout
    else:
        assert expected_zeros in result.stdout
        assert ("Column: 'volume'" in result.output) == ("--no-volume" not in flags)
        assert "Column: 'open'" in result.output
        assert "Column: 'unadjustedValue'" in result.output
        assert "Column: 'split'" in result.output


def test_no_zeros_bypasses_check_and_retains_missing_data_reporting(monkeypatch):
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: pd.DataFrame({"value": [0]}) if endpoint == "div" else pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        check_eod_data, "check_zeros",
        lambda *args, **kwargs: pytest.fail("Zero checks should be disabled"),
    )
    result = CliRunner().invoke(check_eod_data.cli, ["--no-zeros", "--no-eod"])
    assert result.exit_code == 0, result.output
    assert "NO DATA(SPLIT)" in result.stdout
    assert "SKIPPED(EOD)" in result.stdout
    assert "Total Zero Value Issues: SKIPPED" in result.stdout


def test_new_options_are_documented_in_cli_help():
    result = CliRunner().invoke(check_eod_data.cli, ["--help"])
    assert result.exit_code == 0
    assert "--no-volume" in result.output
    assert "--no-zeros" in result.output
    assert "--ticker" in result.output
    assert "--no-clean" in result.output
    assert "--cache" in result.output
    assert "--listed" in result.output
    assert "--delisted" in result.output


@pytest.mark.parametrize(
    ("flags", "expected_tickers"),
    [
        ([], ["LISTED.JSE", "DELISTED.JSE"]),
        (["--listed"], ["LISTED.JSE"]),
        (["--delisted"], ["DELISTED.JSE"]),
    ],
)
def test_listing_status_filters_and_default(monkeypatch, flags, expected_tickers):
    tickers = ["LISTED.JSE", "DELISTED.JSE"]
    calls = []
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: tickers)
    monkeypatch.setattr(
        check_eod_data, "get_listed_status",
        lambda: {"LISTED.JSE": "listed", "DELISTED.JSE": "delisted"},
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: calls.append(ticker) or pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(
        check_eod_data.cli, [*flags, "--no-div", "--no-splits"],
    )
    assert result.exit_code == 0, result.output
    assert calls == expected_tickers


def test_listing_status_filters_are_mutually_exclusive():
    result = CliRunner().invoke(
        check_eod_data.cli, ["--listed", "--delisted"],
    )
    assert result.exit_code == 2
    assert "--listed and --delisted cannot be used together." in result.output


@pytest.mark.parametrize("no_clean", [False, True])
def test_no_clean_filters_only_clean_summary_rows(monkeypatch, no_clean):
    tickers = ["CLEAN.JSE", "ZERO.JSE", "MISSING.JSE", "FAILED.JSE", "NODIV.JSE"]
    calls = []

    def fetch(ticker, endpoint):
        calls.append((ticker, endpoint))
        if ticker == "FAILED.JSE" and endpoint == "eod":
            return None
        if ticker == "MISSING.JSE" and endpoint == "eod":
            return pd.DataFrame()
        if ticker == "NODIV.JSE" and endpoint == "div":
            return pd.DataFrame()
        value = 0 if ticker == "ZERO.JSE" and endpoint == "eod" else 1
        return pd.DataFrame({"date": ["2024-01-01"], "close": [value], "value": [1]})

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: tickers)
    monkeypatch.setattr(check_eod_data, "get_listed_status", lambda: {
        "CLEAN.JSE": "listed", "ZERO.JSE": "listed", "MISSING.JSE": "delisted",
        "FAILED.JSE": "delisted",
    })
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)
    args = ["--verb", "1", "--no-splits"]
    if no_clean:
        args.append("--no-clean")
    result = CliRunner().invoke(check_eod_data.cli, args)
    assert result.exit_code == 0, result.output
    assert len(calls) == 10
    assert "--- Checking CLEAN.JSE ---" in result.stdout
    summary = result.stdout.split("SUMMARY REPORT", 1)[1]
    rows = {line.split()[0]: line for line in summary.splitlines()
            if line.split() and line.split()[0] in tickers}
    assert ("CLEAN.JSE" in rows) is not no_clean
    assert "ZEROS(EOD: 1)" in rows["ZERO.JSE"]
    assert "NO DATA(EOD)" in rows["MISSING.JSE"]
    assert "FETCH FAILED(EOD)" in rows["FAILED.JSE"]
    assert "NO DATA(DIV)" in rows["NODIV.JSE"]
    assert "Total Tickers Checked: 5" in summary
    assert "  Listed: 2" in summary
    assert "  Delisted: 2" in summary
    assert "  Unknown: 1" in summary
    assert "Tickers with Issues: 1" in summary
    assert "Total Zero Value Issues: 1" in summary


def test_no_clean_when_every_ticker_is_clean(monkeypatch):
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["CLEAN.JSE"])
    monkeypatch.setattr(check_eod_data, "get_listed_status", lambda: {"CLEAN.JSE": "listed"})
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: pd.DataFrame({"date": ["2024-01-01"], "close": [1]}),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)
    result = CliRunner().invoke(
        check_eod_data.cli, ["--no-clean", "--no-zeros", "--no-div", "--no-splits"],
    )
    assert result.exit_code == 0, result.output
    summary = result.stdout.split("SUMMARY REPORT", 1)[1]
    assert "CLEAN.JSE" not in summary
    assert "Total Tickers Checked: 1" in summary
    assert "  Listed: 1" in summary
    assert "Total Zero Value Issues: SKIPPED" in summary


def test_cli_audits_selected_tickers_without_loading_default_list(monkeypatch):
    calls = []
    monkeypatch.setattr(
        check_eod_data, "get_etf_tickers",
        lambda: pytest.fail("The default ticker list must not be loaded"),
    )
    monkeypatch.setattr(
        check_eod_data, "get_listed_status",
        lambda: {"FIRST.JSE": "listed", "SECOND.JSE": "delisted", "OTHER.JSE": pd.NA},
    )
    monkeypatch.setattr(
        check_eod_data, "get_distributions_dict",
        lambda: {"FIRST.JSE": True, "SECOND.JSE": False},
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: calls.append((ticker, endpoint)) or pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(
        check_eod_data.cli,
        ["--ticker", "first", "--ticker", "SECOND", "--ticker", "OTHER"],
    )
    assert result.exit_code == 0, result.output
    assert calls == [
        ("FIRST.JSE", "eod"), ("FIRST.JSE", "div"), ("FIRST.JSE", "splits"),
        ("SECOND.JSE", "eod"), ("SECOND.JSE", "splits"),
        ("OTHER.JSE", "eod"), ("OTHER.JSE", "div"), ("OTHER.JSE", "splits"),
    ]
    assert "Unknown distribution expectation for OTHER.JSE" in result.output
    assert "FIRST.JSE" in result.stdout.split("LISTED TICKERS", 1)[1].split("DELISTED TICKERS", 1)[0]
    assert "SECOND.JSE" in result.stdout.split("DELISTED TICKERS", 1)[1].split("UNKNOWN TICKERS", 1)[0]
    assert "OTHER.JSE" in result.stdout.split("UNKNOWN TICKERS", 1)[1]
    assert "  Unknown: 1" in result.stdout
    assert "Total Tickers Checked: 3" in result.stdout


@pytest.mark.parametrize("ticker", ["", "  ", "ABC.JSE", "ABC.jse", "ABC DEF"])
def test_cli_rejects_invalid_tickers_without_fetching(monkeypatch, ticker):
    monkeypatch.setattr(
        check_eod_data, "get_etf_tickers",
        lambda: pytest.fail("The default ticker list must not be loaded"),
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda *args: pytest.fail("Invalid tickers must not be fetched"),
    )
    result = CliRunner().invoke(check_eod_data.cli, ["--ticker", ticker])
    assert result.exit_code == 2
    assert "without the .JSE suffix" in result.output


def test_cli_test_mode_samples_selected_tickers(monkeypatch):
    calls = []
    monkeypatch.setattr(
        check_eod_data, "get_etf_tickers",
        lambda: pytest.fail("The default ticker list must not be loaded"),
    )
    monkeypatch.setattr(check_eod_data, "get_listed_status", lambda: {})
    monkeypatch.setattr(check_eod_data.random, "shuffle", lambda values: values.reverse())
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: calls.append(ticker) or pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)
    args = ["--test", "--no-div", "--no-splits"]
    for i in range(12):
        args.extend(["--ticker", f"TICKER{i}"])
    result = CliRunner().invoke(check_eod_data.cli, args)
    assert result.exit_code == 0, result.output
    assert calls == [f"TICKER{i}.JSE" for i in range(11, 1, -1)]


def test_cache_is_not_read_without_cache_option_but_fetched_data_is_saved(monkeypatch):
    fetched = pd.DataFrame({"date": ["2024-01-01"], "close": [1]})
    saved = []
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(
        check_eod_data, "load_all_cached_data",
        lambda: pytest.fail("Cache should not be read unless --cache is enabled"),
    )
    monkeypatch.setattr(
        check_eod_data, "save_data_to_tmp",
        lambda ticker, endpoint, data: saved.append((ticker, endpoint, data)),
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: fetched,
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(
        check_eod_data.cli, ["--no-div", "--no-splits"],
    )
    assert result.exit_code == 0, result.output
    assert saved == [("FIRST.JSE", "eod", fetched)]


def test_cache_option_uses_existing_cache(monkeypatch):
    cached_eod = pd.DataFrame({"date": ["2024-01-01"], "close": [1]})
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(
        check_eod_data, "load_all_cached_data",
        lambda: {("FIRST.JSE", "eod"): cached_eod},
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda *args: pytest.fail("A cache hit should not fetch data"),
    )
    monkeypatch.setattr(
        check_eod_data, "save_data_to_tmp",
        lambda *args: pytest.fail("A cache hit should not be saved again"),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(
        check_eod_data.cli, ["--cache", "--no-div", "--no-splits"],
    )
    assert result.exit_code == 0, result.output
    assert "Loaded 1 cached data file(s)" in result.output


def test_cache_option_saves_newly_fetched_data(monkeypatch):
    fetched_eod = pd.DataFrame({"date": ["2024-01-01"], "close": [1]})
    saved = []
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(check_eod_data, "load_all_cached_data", lambda: {})
    monkeypatch.setattr(
        check_eod_data, "fetch_data", lambda ticker, endpoint: fetched_eod,
    )
    monkeypatch.setattr(
        check_eod_data, "save_data_to_tmp",
        lambda ticker, endpoint, data: saved.append((ticker, endpoint, data)),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(
        check_eod_data.cli, ["--cache", "--no-div", "--no-splits"],
    )
    assert result.exit_code == 0, result.output
    assert saved == [("FIRST.JSE", "eod", fetched_eod)]


@pytest.mark.parametrize("args", [["--verb", "-1"], ["--verb", "2"], ["--verb", "x"],
                                  ["--no-eod", "--no-div", "--no-splits"]])
def test_cli_rejects_invalid_options_without_fetching(monkeypatch, args):
    def fail_if_called(*args):
        pytest.fail("No API requests should be made for invalid options")

    monkeypatch.setattr(check_eod_data, "fetch_data", fail_if_called)
    result = CliRunner().invoke(check_eod_data.cli, args)
    assert result.exit_code == 2
    assert "Error:" in result.output


@pytest.mark.parametrize(("size", "test_mode"), [(15, True), (5, True), (15, False)])
def test_cli_test_mode_samples_without_modifying_tickers(monkeypatch, size, test_mode):
    tickers = [f"TICKER{i}.JSE" for i in range(size)]
    calls = []
    shuffle_calls = []

    def shuffle(values):
        shuffle_calls.append(True)
        values.reverse()

    def fetch(ticker, endpoint):
        calls.append((ticker, endpoint))
        return pd.DataFrame()

    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: tickers)
    monkeypatch.setattr(check_eod_data.random, "shuffle", shuffle)
    monkeypatch.setattr(check_eod_data, "fetch_data", fetch)
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    args = ["--no-div", "--no-splits"]
    if test_mode:
        args.append("--test")
    result = CliRunner().invoke(check_eod_data.cli, args)
    expected = list(reversed(tickers))[:10] if test_mode else tickers
    assert result.exit_code == 0, result.output
    assert calls == [(ticker, "eod") for ticker in expected]
    assert tickers == [f"TICKER{i}.JSE" for i in range(size)]
    assert len(shuffle_calls) == int(test_mode)
    assert f"Total Tickers Checked: {len(expected)}" in result.output


def test_get_etf_tickers_formats_only_jse_tickers(monkeypatch):
    class Metadata:
        def get_etfs_meta(self):
            return pd.DataFrame({"mic": ["XJSE", "OTHER"], "ticker": ["ABC", "DEF"]})

    monkeypatch.setattr(check_eod_data, "MetaData", Metadata)
    assert check_eod_data.get_etf_tickers() == ["ABC.JSE"]


def test_dividends_are_skipped_only_when_distributions_are_not_expected(monkeypatch):
    calls = []
    tickers = ["PAYS.JSE", "ACCUM.JSE", "UNKNOWN.JSE"]
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: tickers)
    monkeypatch.setattr(
        check_eod_data, "get_distributions_dict",
        lambda: {"PAYS.JSE": True, "ACCUM.JSE": False},
    )
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: calls.append((ticker, endpoint)) or pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(check_eod_data.cli)
    assert result.exit_code == 0, result.output
    assert ("PAYS.JSE", "div") in calls
    assert ("ACCUM.JSE", "div") not in calls
    assert ("UNKNOWN.JSE", "div") in calls
    assert all((ticker, endpoint) in calls for ticker in tickers for endpoint in ("eod", "splits"))
    summary = result.stdout.split("SUMMARY REPORT", 1)[1]
    accum = next(line for line in summary.splitlines() if line.startswith("ACCUM.JSE"))
    pays = next(line for line in summary.splitlines() if line.startswith("PAYS.JSE"))
    assert "SKIPPED(DIV: no distributions expected)" in accum
    assert "NO DATA(EOD, SPLIT)" in accum
    assert "NO DATA(EOD, DIV, SPLIT)" in pays
    assert "Unknown distribution expectation for UNKNOWN.JSE" in result.output


@pytest.mark.parametrize("value", [None, float("nan"), "False"])
def test_unknown_distribution_values_warn_and_check_dividends(monkeypatch, value):
    calls = []
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["UNKNOWN.JSE"])
    monkeypatch.setattr(check_eod_data, "get_distributions_dict", lambda: {"UNKNOWN.JSE": value})
    monkeypatch.setattr(
        check_eod_data, "fetch_data",
        lambda ticker, endpoint: calls.append(endpoint) or pd.DataFrame(),
    )
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(check_eod_data.cli, ["--no-eod", "--no-splits"])
    assert result.exit_code == 0, result.output
    assert calls == ["div"]
    assert "Unknown distribution expectation for UNKNOWN.JSE" in result.output
    assert "NO DATA(DIV)" in result.output


def test_get_distributions_dict_filters_and_formats_metadata(monkeypatch):
    class Metadata:
        def get_etfs_meta(self):
            return pd.DataFrame({
                "mic": ["XJSE", "XJSE", "OTHER"],
                "ticker": ["PAYS", "ACCUM", "OTHER"],
                "distributions": [True, False, True],
            })

    monkeypatch.setattr(check_eod_data, "MetaData", Metadata)
    distributions = get_distributions_dict()
    assert distributions == {"PAYS.JSE": True, "ACCUM.JSE": False}
    assert distributions["PAYS.JSE"] is True
    assert distributions["ACCUM.JSE"] is False


def test_no_div_option_does_not_load_distribution_metadata(monkeypatch):
    monkeypatch.setattr(check_eod_data, "get_etf_tickers", lambda: ["FIRST.JSE"])
    monkeypatch.setattr(
        check_eod_data, "get_distributions_dict",
        lambda: pytest.fail("Distribution metadata is not needed when dividends are disabled"),
    )
    monkeypatch.setattr(check_eod_data, "fetch_data", lambda ticker, endpoint: pd.DataFrame())
    monkeypatch.setattr(check_eod_data.time, "sleep", lambda _: None)

    result = CliRunner().invoke(check_eod_data.cli, ["--no-div"])
    assert result.exit_code == 0, result.output
    assert "SKIPPED(DIV)" in result.output

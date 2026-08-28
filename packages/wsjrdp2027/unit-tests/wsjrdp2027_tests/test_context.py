from __future__ import annotations

import collections as _collections
import gc
import logging
import pathlib as _pathlib
import weakref

import pytest
from wsjrdp2027._context import (
    WsjRdpContext,
    WsjRdpContextConfig,
    _merge_dicts,
    get_thread_local_ctx,
    set_thread_local_ctx,
)


def _touch(path: _pathlib.Path, content: str = "") -> _pathlib.Path:
    """Create a file at *path* with *content* and return it."""
    path.write_text(content, encoding="utf-8")
    return path


class Test_Context_ContextManager:
    @pytest.fixture
    def ctx(self, wsjrdp_context):
        return wsjrdp_context

    def test___del__(self, wsjrdp_config, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        ctx_ref = weakref.ref(ctx)
        del ctx
        gc.collect()
        assert ctx_ref() is None
        captured_log = caplog.text
        assert "Finished context cleanup (level=0)" in captured_log

    def test__get_current_ctx__nothing_set(self, wsjrdp_config):
        ctx = get_thread_local_ctx()
        assert ctx is None

    def test__get_current_ctx__explicitly_set(self, wsjrdp_config):
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        assert get_thread_local_ctx() is None

        set_thread_local_ctx(ctx)
        assert get_thread_local_ctx() is ctx

    def test__get_current_ctx__not_modified_by_creating(self, wsjrdp_config):
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        set_thread_local_ctx(ctx)
        assert get_thread_local_ctx() is ctx

        ctx2 = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        assert ctx2 is not ctx
        assert get_thread_local_ctx() is ctx

    def test__get_current_ctx__set_by_ctx_cm(self, wsjrdp_config):
        outer = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        ctx2 = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)

        set_thread_local_ctx(outer)

        with ctx2:
            assert get_thread_local_ctx() is ctx2

            with ctx:
                assert get_thread_local_ctx() is ctx

            assert get_thread_local_ctx() is ctx2
        assert get_thread_local_ctx() is outer


class FakeResource:
    """Fake resource with just a ``close()`` method; counts every operation."""

    def __init__(self):
        self.calls = _collections.Counter()

    @property
    def closed(self) -> bool:
        return self.calls["close"] > 0

    def close(self):
        self.calls["close"] += 1


class FakeCmResource(FakeResource):
    """Fake resource that is ALSO a context manager. ``__exit__`` delegates to
    ``close()`` so ``closed`` and the call counts read the same for both
    classes."""

    def __enter__(self):
        self.calls["enter"] += 1
        return self

    def __exit__(self, *exc_info):
        self.calls["exit"] += 1
        self.close()


@pytest.mark.parametrize("resource_class", [FakeResource, FakeCmResource])
class Test_Context_Resources:
    @pytest.fixture
    def ctx(self, wsjrdp_config):
        return WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)

    @pytest.fixture
    def make(self, ctx, resource_class):
        """Create/fetch a fake resource through the context's resource cache."""

        def make(key="fake", force_new=False):
            return ctx._get_or_create_resource(
                key,
                create=lambda **kwargs: resource_class(),
                force_new=force_new,
                audithook=False,
            )

        return make

    def test_cache_hit_and_close_once(self, ctx, make):
        with ctx:
            res = make()
            assert make() is res  # cache hit: same instance, no second create
            # The CM variant was entered via its resource exit stack.
            assert res.calls["enter"] == (1 if isinstance(res, FakeCmResource) else 0)
            assert not res.closed
        # The close is reachable twice (resource exit stack + level exit
        # stack), but ExitStack.close() is idempotent: closed exactly ONCE.
        assert res.calls["close"] == 1

    def test_inner_level_resource_dies_with_its_level(self, ctx, make):
        with ctx:
            outer = make("outer")
            with ctx:
                assert make("outer") is outer  # lookup falls through the layers
                inner = make("inner")  # new key -> created in the INNER layer
            assert inner.closed  # ...and therefore closed with the inner level
            assert not outer.closed  # the outer resource survives it
            assert make("outer") is outer  # and is still cached
        assert outer.calls["close"] == 1

    def test_force_new_same_level_closes_predecessor_immediately(self, ctx, make):
        with ctx:
            first = make()
            second = make(force_new=True)
            assert second is not first
            assert first.closed  # replaced within its OWN level: closed early
            assert make() is second  # the cache serves the replacement
        # Early close + level exit still close each resource exactly once.
        assert first.calls["close"] == 1
        assert second.calls["close"] == 1

    def test_force_new_across_levels_only_shadows(self, ctx, make):
        with ctx:
            outer = make()
            with ctx:
                inner = make(force_new=True)
                assert not outer.closed  # outer level's resource is untouched
                assert make() is inner  # shadowed for the inner level
            assert inner.closed
            assert make() is outer  # the outer entry is visible and alive again
        assert outer.calls["close"] == 1


class Test_find_config_file_paths:
    """Tests for ``WsjRdpContextConfig._find_config_file_paths``.

    The helper only *looks up* paths on the filesystem; it never reads the
    files.  ``paths[0]`` is always the main config file (returned even when it
    does not exist); a ``*.local`` sibling is appended only when it exists as a
    regular file and *local_overrides* is enabled.
    """

    @pytest.fixture
    def cfg_p(self, tmp_path):
        p = tmp_path / "config-example.yml"
        p.write_text("", encoding="utf-8")
        return p

    @pytest.fixture
    def cfg_local_p(self, tmp_path):
        p = tmp_path / "config-example.local.yml"
        p.write_text("", encoding="utf-8")
        return p

    _find = WsjRdpContextConfig._find_config_file_paths

    # Each row: files to create under tmp_path; env mapping (values are file
    # names resolved against tmp_path); the ``path`` argument (a file name ->
    # tmp_path/<name>, or None); extra kwargs for ``_find``; and the expected
    # returned paths (file names resolved against tmp_path).
    @pytest.mark.parametrize(
        "existing, env, path_arg, kwargs, expected",
        [
            # explicit path, no local sibling
            (["config.yml"], {}, "config.yml", {}, ["config.yml"]),
            # explicit path, local sibling present
            (
                ["config.yml", "config.local.yml"],
                {},
                "config.yml",
                {},
                ["config.yml", "config.local.yml"],
            ),
            # local present but local_overrides=False -> ignored
            (
                ["config.yml", "config.local.yml"],
                {},
                "config.yml",
                {"local_overrides": False},
                ["config.yml"],
            ),
            # non-default .yaml suffix -> .local.yaml
            (
                ["config.yaml", "config.local.yaml"],
                {},
                "config.yaml",
                {},
                ["config.yaml", "config.local.yaml"],
            ),
            # production config is treated like any other
            (
                ["config-prod.yml", "config-prod.local.yml"],
                {},
                "config-prod.yml",
                {},
                ["config-prod.yml", "config-prod.local.yml"],
            ),
            # main is already a *.local file -> no nested override
            (
                ["config.local.yml", "config.local.local.yml"],
                {},
                "config.local.yml",
                {},
                ["config.local.yml"],
            ),
            # main need not exist; it is still returned
            ([], {}, "config-missing.yml", {}, ["config-missing.yml"]),
            # local may exist even when main does not
            (
                ["config-missing.local.yml"],
                {},
                "config-missing.yml",
                {},
                ["config-missing.yml", "config-missing.local.yml"],
            ),
            # path=None -> resolved from env, local sibling present
            (
                ["config.yml", "config.local.yml"],
                {"WSJRDP_SCRIPTS_CONFIG": "config.yml"},
                None,
                {},
                ["config.yml", "config.local.yml"],
            ),
            # path=None -> resolved from env, no local sibling
            (
                ["config.yml"],
                {"WSJRDP_SCRIPTS_CONFIG": "config.yml"},
                None,
                {},
                ["config.yml"],
            ),
            # explicit path wins over env
            (
                ["config.yml", "other.yml"],
                {"WSJRDP_SCRIPTS_CONFIG": "other.yml"},
                "config.yml",
                {},
                ["config.yml"],
            ),
            # sibling is looked up in the main's own directory
            (
                ["sub/config.yml", "sub/config.local.yml", "config.local.yml"],
                {},
                "sub/config.yml",
                {},
                ["sub/config.yml", "sub/config.local.yml"],
            ),
        ],
    )
    def test_find_config_file_paths(
        self, tmp_path, existing, env, path_arg, kwargs, expected
    ):
        for name in existing:
            p = tmp_path / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("", encoding="utf-8")
        path = None if path_arg is None else tmp_path / path_arg
        env = {k: str(tmp_path / v) for k, v in env.items()}
        assert self._find(path, env=env, **kwargs) == [
            tmp_path / name for name in expected
        ]

    def test_path_may_be_a_str(self, cfg_p, cfg_local_p):
        assert self._find(str(cfg_p), env={}) == [cfg_p, cfg_local_p]

    @pytest.mark.parametrize(
        "existing, expected",
        [
            # default -> cwd-relative config-dev.yml, no local sibling
            (["config-dev.yml"], ["config-dev.yml"]),
            # default -> config-dev.yml plus its local sibling
            (
                ["config-dev.yml", "config-dev.local.yml"],
                ["config-dev.yml", "config-dev.local.yml"],
            ),
        ],
    )
    def test_default_relative_config_dev_yml(
        self, tmp_path, monkeypatch, existing, expected
    ):
        monkeypatch.chdir(tmp_path)
        for name in existing:
            _touch(tmp_path / name)
        assert self._find(None, env={}) == [_pathlib.Path(n) for n in expected]

    def test_empty_env_var_is_degenerate_but_does_not_raise(self):
        # Empty WSJRDP_SCRIPTS_CONFIG keeps the historical ``is not None``
        # behaviour: "" -> Path("."); the name guard avoids a with_stem() crash.
        assert self._find(None, env={"WSJRDP_SCRIPTS_CONFIG": ""}) == [
            _pathlib.Path(".")
        ]

    def test_local_that_is_a_directory_is_ignored(self, cfg_p, tmp_path):
        (tmp_path / f"{cfg_p.stem}.local.yml").mkdir()  # a dir, not a file
        assert self._find(cfg_p, env={}) == [cfg_p]

    def test_local_that_is_a_dangling_symlink_is_ignored(self, cfg_p, tmp_path):
        link = tmp_path / f"{cfg_p.stem}.local.yml"
        try:
            link.symlink_to(tmp_path / "does-not-exist.yml")
        except Exception:
            pytest.skip("symlinks not supported on this platform")
        assert self._find(cfg_p, env={}) == [cfg_p]


_MAIN_CONFIG_YAML = """\
use_ssh_tunnel: false
db_host: main-db.example
db_port: 5432
db_username: u
db_password: p
db_name: maindb
smtp_server: smtp.example
smtp_port: 587
"""


class Test_from_file_local_override:
    """``from_file`` reads ``.local`` override files and merges them over base."""

    def test_override_is_merged_over_base(self, tmp_path, caplog):
        main = _touch(tmp_path / "config-dev.yml", _MAIN_CONFIG_YAML)
        # overrides db_host/db_name, adds geo_api_key
        _touch(
            tmp_path / "config-dev.local.yml",
            "db_host: local-db.example\ndb_name: localdb\ngeo_api_key: LOCAL-KEY\n",
        )
        caplog.set_level(logging.INFO)
        cfg = WsjRdpContextConfig.from_file(main)
        assert cfg.db_host == "local-db.example"  # override wins
        assert cfg.db_name == "localdb"  # override wins
        assert cfg.geo_api_key == "LOCAL-KEY"  # added by override
        assert cfg.db_username == "u"  # base-only key preserved
        assert "Read config override file" in caplog.text

    def test_no_override_file_leaves_base_untouched(self, tmp_path, caplog):
        main = _touch(tmp_path / "config-dev.yml", _MAIN_CONFIG_YAML)
        caplog.set_level(logging.INFO)
        cfg = WsjRdpContextConfig.from_file(main)
        assert cfg.db_host == "main-db.example"
        assert "Read config override file" not in caplog.text

    def test_local_overrides_false_skips_override(self, tmp_path, caplog):
        main = _touch(tmp_path / "config-dev.yml", _MAIN_CONFIG_YAML)
        _touch(tmp_path / "config-dev.local.yml", "db_host: local-db.example\n")
        caplog.set_level(logging.INFO)
        cfg = WsjRdpContextConfig.from_file(main, local_overrides=False)
        assert cfg.db_host == "main-db.example"  # override ignored
        assert "Read config override file" not in caplog.text

    def test_empty_override_file_is_ignored(self, tmp_path):
        main = _touch(tmp_path / "config-dev.yml", _MAIN_CONFIG_YAML)
        _touch(tmp_path / "config-dev.local.yml", "")  # empty -> yaml.load None
        cfg = WsjRdpContextConfig.from_file(main)
        assert cfg.db_host == "main-db.example"

    def test_env_param_selects_main_config_without_touching_os_environ(self, tmp_path):
        main = _touch(tmp_path / "config-int.yml", _MAIN_CONFIG_YAML)
        cfg = WsjRdpContextConfig.from_file(env={"WSJRDP_SCRIPTS_CONFIG": str(main)})
        assert cfg.db_host == "main-db.example"


class Test_merge_dicts:
    @pytest.mark.parametrize(
        "base, override, expected",
        [
            # override wins on scalars
            ({"a": 1}, {"a": 2}, {"a": 2}),
            # new keys are added
            ({"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
            # nested dicts merge recursively
            (
                {"a": {"x": 1, "y": 2}},
                {"a": {"y": 3, "z": 4}},
                {"a": {"x": 1, "y": 3, "z": 4}},
            ),
            # a non-dict override replaces a dict
            ({"a": {"x": 1}}, {"a": 5}, {"a": 5}),
            # a dict override replaces a non-dict
            ({"a": 5}, {"a": {"x": 1}}, {"a": {"x": 1}}),
            # lists replace, not concatenate
            ({"a": [1, 2]}, {"a": [3]}, {"a": [3]}),
            # empty override leaves base unchanged
            ({"a": 1}, {}, {"a": 1}),
        ],
    )
    def test_merge_dicts(self, base, override, expected):
        assert _merge_dicts(base, override) == expected
        assert base == expected  # merged in place

    def test_merge_dicts_is_in_place(self):
        base = {"a": {"x": 1}}
        nested = base["a"]
        result = _merge_dicts(base, {"a": {"y": 2}})
        assert result is base  # returns the same object
        assert base["a"] is nested  # nested dict mutated in place
        assert base == {"a": {"x": 1, "y": 2}}

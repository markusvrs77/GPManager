# -*- coding: utf-8 -*-
"""systemd-сервис: unit, настройки и переопределение config окружением."""

import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPLOY = os.path.join(ROOT, "deploy")


def _read(name):
    with open(os.path.join(DEPLOY, name), encoding="utf-8") as f:
        return f.read()


def test_restart_does_not_kill_gpcopy():
    """Без KillMode=process systemctl restart оборвал бы gpcopy/gpbackup."""
    unit = _read("opsentri.service")

    assert re.search(r"^KillMode=process$", unit, re.M)
    assert "EnvironmentFile=/etc/opsentri/opsentri.env" in unit


def test_installer_fills_every_placeholder():
    script = _read("install-service.sh")

    for name, text in (("opsentri.service", _read("opsentri.service")),
                       ("opsentri.env.example", _read("opsentri.env.example"))):
        for placeholder in set(re.findall(r"@[A-Z_]+@", text)):
            assert "s|{}|".format(placeholder) in script, (name, placeholder)


def test_server_env_disables_debug():
    assert re.search(r"^OPSENTRI_DEBUG=0$", _read("opsentri.env.example"), re.M)


def _config(env_extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("OPSENTRI_")}
    env.update(env_extra)
    out = subprocess.check_output(
        [sys.executable, "-c",
         "import config; print(config.APP_HOST, config.APP_PORT, config.APP_DEBUG)"],
        cwd=ROOT, env=env, universal_newlines=True)
    return out.split()


def test_config_defaults_unchanged():
    assert _config({}) == ["0.0.0.0", "8080", "True"]


def test_config_from_environment():
    assert _config({"OPSENTRI_HOST": "127.0.0.1", "OPSENTRI_PORT": "9090",
                    "OPSENTRI_DEBUG": "0"}) == ["127.0.0.1", "9090", "False"]

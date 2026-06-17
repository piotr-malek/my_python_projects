#!/usr/bin/env python3
"""Family Lunch Coach CLI — invoked by OpenClaw skill and cron jobs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo without install
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lunch_coach.config import load_settings
from lunch_coach.db import Database, utcnow
from lunch_coach.handler import handle_coach_message
from lunch_coach.nudges import reconcile_missed, run_nudge
from lunch_coach.ollama_client import OllamaClient
from lunch_coach.onboarding import OnboardingFSM, is_skip_message


def cmd_startup(settings) -> int:
    db = Database(settings)
    db.init_schema()
    db.seed_if_empty()
    msgs = reconcile_missed(db, settings)
    db.set_meta("last_run", utcnow())
    for m in msgs:
        print(m)
    return 0


def cmd_heartbeat(settings) -> int:
    db = Database(settings)
    db.init_schema()
    from lunch_coach.onboarding import OnboardingFSM

    llm = OllamaClient(settings)
    ob = OnboardingFSM(db, settings, llm)
    renudge = ob.maybe_renudge()
    if renudge:
        print(renudge)
    msgs = reconcile_missed(db, settings)
    db.set_meta("last_run", utcnow())
    for m in msgs:
        print(m)
    if not renudge and not msgs:
        print("NO_REPLY")
    return 0


def cmd_handle(settings, message: str) -> int:
    db = Database(settings)
    db.init_schema()
    llm = OllamaClient(settings)
    ob = OnboardingFSM(db, settings, llm)

    state = db.get_onboarding()
    if is_skip_message(message):
        ob.skip()
        reply = handle_coach_message(db, settings, llm, message)
        print(reply)
        return 0

    if ob.should_handle(message):
        reply = ob.handle(message)
        if reply:
            print(reply)
            return 0

    reply = handle_coach_message(db, settings, llm, message)
    print(reply)
    return 0


def cmd_nudge(settings, nudge_type: str) -> int:
    db = Database(settings)
    db.init_schema()
    print(run_nudge(db, settings, nudge_type))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Family Lunch Coach")
    parser.add_argument("--config", help="Path to config.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("startup")
    sub.add_parser("heartbeat")

    p_handle = sub.add_parser("handle")
    p_handle.add_argument("message", nargs="+", help="User message")

    p_nudge = sub.add_parser("nudge")
    p_nudge.add_argument("nudge_type", choices=["lunch_reminder", "sunday_planning", "friday_reflection"])

    args = parser.parse_args(argv)
    settings = load_settings(args.config)

    if args.command == "startup":
        return cmd_startup(settings)
    if args.command == "heartbeat":
        return cmd_heartbeat(settings)
    if args.command == "handle":
        return cmd_handle(settings, " ".join(args.message))
    if args.command == "nudge":
        return cmd_nudge(settings, args.nudge_type)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

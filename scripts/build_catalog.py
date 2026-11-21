"""Build the public chat-model snapshot without API credentials."""

import json
import math
import re
import shutil
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
FEEDS = {
    "routerai": "https://routerai.ru/api/v1/models",
    "polza": "https://polza.ai/api/v1/models",
    "aitunnel": "https://api.aitunnel.ru/public/aitunnel/models/chat",
}
FX_SOURCE = "https://www.cbr.ru/scripts/XML_daily.asp"


def read(url):
    with urlopen(Request(url, headers={"User-Agent": "SiftGate-Catalog/1.0"}), timeout=20) as response:
        return response.read()


def amount(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def model_key(model_id):
    # Only normalize the documented dot/hyphen aliases for these Claude versions.
    return re.sub(r"^(anthropic/claude-(?:sonnet|opus|haiku)-\d)-(\d)$", r"\1.\2", model_id)


def offers(provider, data):
    if provider == "aitunnel":
        for model_id, model in data.items():
            if not model.get("provider") or "text" not in model.get("modalities", {}).get("output", []):
                continue
            author = model["provider"]
            key = model_id if "/" in model_id else f"{author}/{model_id}"
            yield offer(provider, model_id, key, model_id, ["chat", "messages"] if author == "anthropic" else ["chat"],
                        "RUB", model.get("prompt_cost"), model.get("completion_cost"), 0, 1)
        return

    for model in data["data"]:
        model_id = model["id"]
        endpoints = model.get("endpoints") or []
        outputs = (model.get("architecture") or {}).get("output_modalities") or []
        if endpoints:
            protocols = [name for name, suffix in [("chat", "/chat/completions"), ("messages", "/messages")]
                         if any(path.endswith(suffix) for path in endpoints)]
        elif provider == "routerai" and "text" in outputs:
            protocols = ["chat", "messages"] if model_id.startswith("anthropic/claude-") else ["chat"]
        else:
            continue
        if not protocols or (outputs and "text" not in outputs):
            continue

        if provider == "polza":
            price = (model.get("top_provider") or {}).get("pricing") or {}
            input_price, output_price = price.get("prompt_per_million"), price.get("completion_per_million")
            currency, scale = price.get("currency", "RUB"), 1
        else:
            price = model.get("pricing") or {}
            units = model.get("pricing_units") or {}
            token_rates = all(units.get(field, "token") == "token" for field in ("prompt", "completion"))
            input_price = price.get("prompt") if token_rates else None
            output_price = price.get("completion") if token_rates else None
            currency, scale = price.get("currency", "RUB"), 1_000_000
        yield offer(provider, model_id, model_id, model.get("name") or model_id, protocols,
                    currency, input_price, output_price, price.get("request", 0), scale)


def offer(provider, model_id, key, name, protocols, currency, input_price, output_price, request_price, scale):
    rates = [amount(value) for value in (input_price, output_price, request_price)]
    price = None
    if currency in ("RUB", "USD") and all(value is not None for value in rates):
        price = dict(currency=currency, input=rates[0] * scale, output=rates[1] * scale, request=rates[2])
    return dict(provider=provider, id=model_id, key=model_key(key), name=name, protocols=protocols, price=price)


def refresh_feed(item):
    provider, url = item
    try:
        rows = list(offers(provider, json.loads(read(url))))
        if not rows or len({row["id"] for row in rows}) != len(rows):
            raise ValueError("empty catalog or duplicate model IDs")
        day = datetime.now(timezone.utc).date().isoformat()
        return provider, dict(date=day, url=url, offers=rows, stale=False)
    except Exception as error:
        print(f"{provider}: {error}", file=sys.stderr)
        return provider, None


def refresh(snapshot):
    with ThreadPoolExecutor() as pool:
        for provider, result in pool.map(refresh_feed, FEEDS.items()):
            if result:
                snapshot["feeds"][provider] = result
            elif provider in snapshot["feeds"]:
                snapshot["feeds"][provider]["stale"] = True
    try:
        rates = ET.fromstring(read(FX_SOURCE))
        usd = rates.find("Valute[CharCode='USD']")
        rate = amount(usd.findtext("Value").replace(",", "."))
        nominal = amount(usd.findtext("Nominal"))
        if not rate or not nominal:
            raise ValueError("invalid rate")
        date = datetime.strptime(rates.attrib["Date"], "%d.%m.%Y").date().isoformat()
        snapshot["fx"] = dict(rate=rate / nominal, date=date, url=FX_SOURCE, stale=False)
    except Exception as error:
        print(f"CBR: {error}", file=sys.stderr)
        if snapshot["fx"]:
            snapshot["fx"]["stale"] = True


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--out", type=Path, default=ROOT / "site-dist")
    parser.add_argument("--seed", action="store_true", help="replace the checked-in snapshot")
    args = parser.parse_args()
    snapshot_path = ROOT / "site/catalog.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if args.refresh:
        refresh(snapshot)
    if args.seed:
        snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    shutil.copytree(ROOT / "site", args.out, dirs_exist_ok=True)
    shutil.copy2(ROOT / "sift-gate.ru.example.toml", args.out)
    (args.out / "catalog.json").write_text(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(", ".join(f"{provider}: {len(feed['offers'])}" for provider, feed in snapshot["feeds"].items()))


if __name__ == "__main__":
    main()

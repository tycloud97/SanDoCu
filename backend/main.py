import argparse
import logging
import sys


def run_fb_group(num_posts: int, headless: bool) -> bool:
    # Lazy import to avoid importing heavy deps unless needed
    from facebook_group_crawler import run_fb_group_crawler

    return run_fb_group_crawler(num_posts=num_posts, headless=headless)


def run_chotot(start_page: int, end_page: int) -> bool:
    from chotot_crawler import run_crawler

    return run_crawler(start_page, end_page)


def run_fb_marketplace() -> bool:
    from facebook_marketplace_crawler import run_fb_crawler

    return run_fb_crawler()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SanDoCu crawlers CLI. Choose a mode to run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "mode",
        help=(
            "Crawler mode: 1=fb-group, 2=chotot, 3=fb-marketplace. "
            "You can also pass names: fb-group|chotot|fb-market."
        ),
        choices=["1", "2", "3", "fb-group", "chotot", "fb-market", "fb-marketplace"],
    )

    # Common options (not all will be used by every mode)
    parser.add_argument("--headless", action="store_true", help="Run browsers headless if supported")
    parser.add_argument("--count", type=int, default=20, help="Number of group posts (mode 1)")
    parser.add_argument("--start", type=int, default=1, help="Start page (mode 2)")
    parser.add_argument("--end", type=int, default=1, help="End page (mode 2)")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    mode = args.mode
    ok = False
    if mode in ("1", "fb-group"):
        ok = run_fb_group(num_posts=args.count, headless=args.headless)
    elif mode in ("2", "chotot"):
        ok = run_chotot(start_page=args.start, end_page=args.end)
    elif mode in ("3", "fb-market"):
        ok = run_fb_marketplace()
    else:
        parser.error("Unknown mode")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

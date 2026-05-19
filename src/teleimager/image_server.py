"""Backward-compatible imports for TeleImager server APIs."""

from teleimager.video_streaming.server import *  # noqa: F403
from teleimager.video_streaming.server import main


if __name__ == "__main__":
    main()

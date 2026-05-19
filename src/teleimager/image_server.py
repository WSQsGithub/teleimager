"""Backward-compatible imports for TeleImager server APIs."""

from teleimager.video_streaming import server as _server

main = _server.main
__all__ = list(getattr(_server, "__all__", []))


def __getattr__(name):
    return getattr(_server, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_server)))


if __name__ == "__main__":
    main()

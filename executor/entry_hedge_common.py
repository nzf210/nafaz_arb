"""Shared exception types for the entry hedge lifecycle."""

class AmbiguousOrderSubmission(RuntimeError):
    """The venue may have accepted an order but its acknowledgement is unknown."""

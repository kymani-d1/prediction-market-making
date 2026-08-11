from __future__ import annotations

from decimal import Decimal

import pytest

from prediction_collector.common.orderbook import OrderBook, kalshi_yes_book


def test_orderbook_uses_exact_decimals_and_sorted_serialisation() -> None:
    book = OrderBook()
    book.reset(
        [["0.40", "1.25"], {"price": "0.42", "size": "3.50"}],
        [["0.46", "4"], {"price": "0.44", "size": "2.75"}],
        sequence=10,
        book_hash="snapshot-hash",
    )

    assert book.valid
    assert book.sequence == 10
    assert book.book_hash == "snapshot-hash"
    assert book.best_bid == Decimal("0.42")
    assert book.best_ask == Decimal("0.44")
    assert book.midpoint == Decimal("0.43")
    assert book.spread == Decimal("0.02")
    assert book.bid_depth == Decimal("4.75")
    assert book.ask_depth == Decimal("6.75")
    assert book.serialise_bids() == [["0.42", "3.50"], ["0.40", "1.25"]]
    assert book.serialise_asks() == [["0.44", "2.75"], ["0.46", "4"]]


def test_absolute_and_delta_updates_set_delete_and_advance_sequence() -> None:
    book = OrderBook()
    book.reset([["0.40", "2"]], [["0.60", "3"]], sequence=1)

    book.apply_absolute("BUY", Decimal("0.41"), Decimal("4.5"), sequence=2)
    book.apply_absolute("bid", Decimal("0.40"), Decimal("0"), sequence=3)
    book.apply_delta("ask", Decimal("0.60"), Decimal("-1.25"), sequence=4)
    book.apply_delta("sell", Decimal("0.61"), Decimal("2.5"), sequence=5)

    assert book.bids == {Decimal("0.41"): Decimal("4.5")}
    assert book.asks == {
        Decimal("0.60"): Decimal("1.75"),
        Decimal("0.61"): Decimal("2.5"),
    }
    assert book.sequence == 5

    book.apply_delta("ask", Decimal("0.60"), Decimal("-1.75"))
    assert Decimal("0.60") not in book.asks


def test_mutation_without_authoritative_hash_clears_snapshot_hash() -> None:
    book = OrderBook()
    book.reset([["0.40", "2"]], [["0.60", "3"]], book_hash="snapshot-H")

    book.apply_absolute("bid", Decimal("0.41"), Decimal("1"))

    assert book.book_hash is None


def test_unknown_orderbook_side_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown book side"):
        OrderBook().apply_absolute("maybe", Decimal("0.5"), Decimal("1"))


def test_kalshi_no_bids_become_exact_yes_asks() -> None:
    bids, asks = kalshi_yes_book(
        [["0.42", "3.250"], ["0.40", "8.125"]],
        [["0.45", "2.50"], ["0.40", "4"]],
    )

    assert bids == [["0.42", "3.250"], ["0.40", "8.125"]]
    assert asks == [["0.55", "2.50"], ["0.60", "4"]]


def test_non_positive_and_invalid_levels_do_not_enter_book() -> None:
    book = OrderBook()
    book.reset(
        [["0.4", "0"], ["0.3", "-1"], ["bad", "2"], ["0.2", "1"]],
        [None, {"price": "0.8", "quantity": "2.5"}],
    )

    assert book.bids == {Decimal("0.2"): Decimal("1")}
    assert book.asks == {Decimal("0.8"): Decimal("2.5")}

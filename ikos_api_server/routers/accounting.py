from __future__ import annotations

import csv
import io
import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

import domain_db
import object_db
from ikos_api_server.deps import require_permission


router = APIRouter(
    prefix="/api/accounting",
    tags=["accounting"],
    dependencies=[Depends(require_permission("finance.read"))],
)
LOGGER = logging.getLogger("idms.api")


class BookingReviewApproveRequest(BaseModel):
    reviewed_by: str = "api:user"
    review_note: str = ""
    booking_updates: dict[str, Any] = Field(default_factory=dict)


class BookingReviewCancelRequest(BaseModel):
    reviewed_by: str = "api:user"
    review_note: str = ""


def _parse_iso_date(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: {text}") from exc


@router.get("/journal")
def get_journal(
    start_date: str,
    end_date: str,
    legal_entity_ref: str | None = None,
    account_number: int | None = None,
    direction: str | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> dict[str, Any]:
    start = _parse_iso_date(start_date, "start_date")
    end = _parse_iso_date(end_date, "end_date")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    direction_norm = str(direction or "").strip().lower() or None
    if direction_norm not in {None, "debit", "credit"}:
        raise HTTPException(status_code=400, detail="direction must be debit or credit")

    connection = object_db.get_connection()
    try:
        rows = domain_db.list_journal_lines(
            connection=connection,
            start_date=start,
            end_date=end,
            legal_entity_ref=legal_entity_ref,
            account_number=account_number,
            direction=direction_norm,
            limit=limit,
            offset=offset,
        )

        tx_map: dict[str, dict[str, Any]] = {}
        for row in rows:
            tx_id = str(row.get("transaction_id") or "")
            tx = tx_map.get(tx_id)
            if tx is None:
                tx = {
                    "transaction_id": tx_id,
                    "journal_sequence_number": row.get("journal_sequence_number"),
                    "transaction_date": row.get("transaction_date"),
                    "legal_entity_ref": row.get("legal_entity_ref"),
                    "source_type": row.get("source_type"),
                    "source_document_id": row.get("source_document_id"),
                    "object_id": row.get("object_id"),
                    "description": row.get("description"),
                    "receipt_archive_url": row.get("receipt_archive_url"),
                    "created_at": row.get("created_at"),
                    "lines": [],
                }
                tx_map[tx_id] = tx

            tx["lines"].append(
                {
                    "line_id": row.get("line_id"),
                    "account_number": row.get("account_number"),
                    "account_name": row.get("account_name"),
                    "account_type": row.get("account_type"),
                    "direction": row.get("direction"),
                    "source_currency": row.get("source_currency"),
                    "amount_source_currency": row.get("amount_source_currency"),
                    "exchange_rate_to_chf": row.get("exchange_rate_to_chf"),
                    "amount_chf": row.get("amount_chf"),
                    "swiss_vat_code": row.get("swiss_vat_code"),
                    "swiss_vat_box": row.get("swiss_vat_box"),
                    "metadata": row.get("line_metadata") or {},
                }
            )

        journal = sorted(
            tx_map.values(),
            key=lambda tx: (
                str(tx.get("transaction_date") or ""),
                int(tx.get("journal_sequence_number") or 0),
            ),
        )

        return {
            "success": True,
            "count_lines": len(rows),
            "count_transactions": len(journal),
            "result": journal,
            "query": {
                "start_date": start,
                "end_date": end,
                "legal_entity_ref": legal_entity_ref,
                "account_number": account_number,
                "direction": direction_norm,
                "limit": limit,
                "offset": offset,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to fetch accounting journal")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/vat-return")
def get_vat_return(
    start_date: str,
    end_date: str,
    legal_entity_ref: str | None = None,
) -> dict[str, Any]:
    start = _parse_iso_date(start_date, "start_date")
    end = _parse_iso_date(end_date, "end_date")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    connection = object_db.get_connection()
    try:
        result = domain_db.summarize_vat_return(
            connection=connection,
            start_date=start,
            end_date=end,
            legal_entity_ref=legal_entity_ref,
        )
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to summarize VAT return")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/journal/export", response_class=PlainTextResponse)
def export_journal_csv(
    start_date: str,
    end_date: str,
    legal_entity_ref: str | None = None,
    account_number: int | None = None,
    direction: str | None = None,
    limit: int = 10000,
    offset: int = 0,
) -> str:
    start = _parse_iso_date(start_date, "start_date")
    end = _parse_iso_date(end_date, "end_date")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    direction_norm = str(direction or "").strip().lower() or None
    if direction_norm not in {None, "debit", "credit"}:
        raise HTTPException(status_code=400, detail="direction must be debit or credit")

    connection = object_db.get_connection()
    try:
        rows = domain_db.list_journal_lines(
            connection=connection,
            start_date=start,
            end_date=end,
            legal_entity_ref=legal_entity_ref,
            account_number=account_number,
            direction=direction_norm,
            limit=limit,
            offset=offset,
        )

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "transaction_id",
                "journal_sequence_number",
                "transaction_date",
                "legal_entity_ref",
                "source_type",
                "source_document_id",
                "object_id",
                "description",
                "line_id",
                "account_number",
                "account_name",
                "account_type",
                "direction",
                "source_currency",
                "amount_source_currency",
                "exchange_rate_to_chf",
                "amount_chf",
                "swiss_vat_code",
                "swiss_vat_box",
                "created_at",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("transaction_id"),
                    row.get("journal_sequence_number"),
                    row.get("transaction_date"),
                    row.get("legal_entity_ref"),
                    row.get("source_type"),
                    row.get("source_document_id"),
                    row.get("object_id"),
                    row.get("description"),
                    row.get("line_id"),
                    row.get("account_number"),
                    row.get("account_name"),
                    row.get("account_type"),
                    row.get("direction"),
                    row.get("source_currency"),
                    row.get("amount_source_currency"),
                    row.get("exchange_rate_to_chf"),
                    row.get("amount_chf"),
                    row.get("swiss_vat_code"),
                    row.get("swiss_vat_box"),
                    row.get("created_at"),
                ]
            )

        return output.getvalue()
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to export accounting journal")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/reviews")
def list_booking_reviews(
    status: str | None = "pending",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    status_norm = str(status or "").strip().lower() if status is not None else ""
    if status_norm and status_norm not in {"pending", "approved", "cancelled"}:
        raise HTTPException(status_code=400, detail="status must be pending, approved, or cancelled")

    connection = object_db.get_connection()
    try:
        rows = domain_db.list_accounting_booking_reviews(
            connection=connection,
            status=status_norm or None,
            limit=limit,
            offset=offset,
        )
        return {
            "success": True,
            "count": len(rows),
            "result": rows,
            "query": {
                "status": status_norm or None,
                "limit": int(limit),
                "offset": int(offset),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to list accounting booking reviews")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.get("/reviews/{review_id}")
def get_booking_review(review_id: int) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        row = domain_db.get_accounting_booking_review(connection=connection, review_id=review_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Review {review_id} not found")
        return {"success": True, "result": row}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to get accounting booking review")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/reviews/{review_id}/approve", dependencies=[Depends(require_permission("finance.write"))])
def approve_booking_review(review_id: int, payload: BookingReviewApproveRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = domain_db.approve_accounting_booking_review(
            connection=connection,
            review_id=review_id,
            booking_updates=payload.booking_updates,
            reviewed_by=payload.reviewed_by,
            review_note=payload.review_note,
        )
        if not bool(result.get("ok")):
            reason = str(result.get("reason") or "").strip().lower()
            if reason == "not_found":
                raise HTTPException(status_code=404, detail=f"Review {review_id} not found")
            if reason == "not_pending":
                raise HTTPException(status_code=409, detail=f"Review {review_id} is not pending")
            if reason == "validation_failed":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Booking validation failed. Please update booking data and try again.",
                        "validation": result.get("validation") or {},
                    },
                )
            raise HTTPException(status_code=500, detail=result)
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to approve accounting booking review")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()


@router.post("/reviews/{review_id}/cancel", dependencies=[Depends(require_permission("finance.write"))])
def cancel_booking_review(review_id: int, payload: BookingReviewCancelRequest) -> dict[str, Any]:
    connection = object_db.get_connection()
    try:
        result = domain_db.cancel_accounting_booking_review(
            connection=connection,
            review_id=review_id,
            reviewed_by=payload.reviewed_by,
            review_note=payload.review_note,
        )
        if not bool(result.get("ok")):
            reason = str(result.get("reason") or "").strip().lower()
            if reason == "not_found":
                raise HTTPException(status_code=404, detail=f"Review {review_id} not found")
            if reason == "not_pending":
                raise HTTPException(status_code=409, detail=f"Review {review_id} is not pending")
            raise HTTPException(status_code=500, detail=result)
        return {"success": True, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        LOGGER.exception("Failed to cancel accounting booking review")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        connection.close()

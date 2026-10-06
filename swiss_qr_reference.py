from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_SHIFT_TABLE: list[list[int]] = [
    [0, 9, 4, 6, 8, 2, 7, 1, 3, 5],
    [9, 4, 6, 8, 2, 7, 1, 3, 5, 0],
    [4, 6, 8, 2, 7, 1, 3, 5, 0, 9],
    [6, 8, 2, 7, 1, 3, 5, 0, 9, 4],
    [8, 2, 7, 1, 3, 5, 0, 9, 4, 6],
    [2, 7, 1, 3, 5, 0, 9, 4, 6, 8],
    [7, 1, 3, 5, 0, 9, 4, 6, 8, 2],
    [1, 3, 5, 0, 9, 4, 6, 8, 2, 7],
    [3, 5, 0, 9, 4, 6, 8, 2, 7, 1],
    [5, 0, 9, 4, 6, 8, 2, 7, 1, 3],
]


@dataclass(frozen=True)
class SwissQRReference:
    """Represents a normalized Swiss QR reference (QRR).

    - storage: 27 digits (machine / DB / QR payload)
    - visual: grouped as 2 + 5 + 5 + 5 + 5 + 5 digits
    - payload: first 26 digits (without check digit)
    """

    storage: str

    @property
    def payload(self) -> str:
        return self.storage[:26]

    @property
    def check_digit(self) -> str:
        return self.storage[26]

    @property
    def visual(self) -> str:
        raw = self.storage
        return f"{raw[:2]} {raw[2:7]} {raw[7:12]} {raw[12:17]} {raw[17:22]} {raw[22:]}"


class SwissQRReferenceGenerator:
    """Generic Swiss QR reference generator + validator.

    Supports these common flows:
    1) Generate from an integer sequence.
    2) Generate from payload digits (26 digits).
    3) Generate next reference from an existing 27-digit reference.
    4) Generate from prefix+sequence so invoice numbering rules can be mapped.
    """

    def compact(self, value: str) -> str:
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        if not digits:
            raise ValueError("reference must contain at least one digit")
        return digits

    def calculate_check_digit(self, payload: str) -> int:
        payload_digits = self.compact(payload)
        if len(payload_digits) != 26:
            raise ValueError("payload must be exactly 26 digits")

        carry = 0
        for ch in payload_digits:
            carry = _SHIFT_TABLE[carry][int(ch)]
        return (10 - carry) % 10

    def from_payload(self, payload: str) -> SwissQRReference:
        payload_digits = self.compact(payload)
        if len(payload_digits) != 26:
            raise ValueError("payload must be exactly 26 digits")
        check_digit = self.calculate_check_digit(payload_digits)
        return SwissQRReference(storage=f"{payload_digits}{check_digit}")

    def from_sequence(self, sequence: int) -> SwissQRReference:
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        if sequence > (10**26 - 1):
            raise ValueError("sequence does not fit into 26 digits")
        return self.from_payload(f"{sequence:026d}")

    def from_prefix_and_sequence(self, prefix: str, sequence: int) -> SwissQRReference:
        """Build reference from a fixed prefix and a variable sequence.

        Example:
        - prefix: first digits tied to your business logic or account partition
        - sequence: increasing number tied to invoices

        The combined payload must be exactly 26 digits.
        """

        prefix_digits = "".join(ch for ch in str(prefix or "") if ch.isdigit())
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        if len(prefix_digits) > 25:
            raise ValueError("prefix is too long; max 25 digits")

        seq_width = 26 - len(prefix_digits)
        if sequence > (10**seq_width - 1):
            raise ValueError(f"sequence does not fit into remaining {seq_width} digits")
        payload = f"{prefix_digits}{sequence:0{seq_width}d}"
        return self.from_payload(payload)

    def validate(self, reference: str) -> bool:
        try:
            digits = self.compact(reference)
        except ValueError:
            return False
        if len(digits) != 27:
            return False
        payload = digits[:26]
        expected = self.calculate_check_digit(payload)
        return expected == int(digits[26])

    def parse(self, reference: str) -> SwissQRReference:
        digits = self.compact(reference)
        if len(digits) != 27:
            raise ValueError("reference must be exactly 27 digits")
        if not self.validate(digits):
            raise ValueError("invalid Swiss QR reference check digit")
        return SwissQRReference(storage=digits)

    def next_from_sequence(self, current_sequence: int, step: int = 1) -> dict[str, int | str]:
        if step <= 0:
            raise ValueError("step must be positive")
        if current_sequence < 0:
            raise ValueError("current_sequence must be non-negative")
        next_sequence = current_sequence + step
        ref = self.from_sequence(next_sequence)
        return {
            "next_sequence_to_store": next_sequence,
            "storage_string": ref.storage,
            "visual_string": ref.visual,
        }

    def next_from_reference(self, current_reference: str, step: int = 1) -> SwissQRReference:
        """Increment the payload portion (first 26 digits) and recalculate check digit."""

        if step <= 0:
            raise ValueError("step must be positive")
        current = self.parse(current_reference)
        payload_value = int(current.payload)
        next_payload = payload_value + step
        if next_payload > (10**26 - 1):
            raise ValueError("payload overflow: cannot exceed 26 digits")
        return self.from_sequence(next_payload)

    def next_from_reference_with_fixed_segments(
        self,
        current_reference: str,
        fixed_segments: list[dict[str, Any]] | None = None,
        step: int = 1,
    ) -> SwissQRReference:
        """Increment only mutable payload digits while keeping configured segments fixed.

        Segment positions are 1-based and refer to the 26-digit payload, not the 27th check digit.
        Example fixed segment: {"start": 21, "value": "26"}.
        """

        if step <= 0:
            raise ValueError("step must be positive")

        current = self.parse(current_reference)
        payload = list(current.payload)

        fixed_positions: dict[int, str] = {}
        for segment in fixed_segments or []:
            if not isinstance(segment, dict):
                continue
            start_raw = segment.get("start")
            value_digits = "".join(ch for ch in str(segment.get("value") or "") if ch.isdigit())
            if not value_digits:
                continue
            try:
                start = int(start_raw)
            except Exception as exc:
                raise ValueError("fixed segment start must be an integer") from exc
            if start < 1 or start > 26:
                raise ValueError("fixed segment start must be within payload positions 1..26")
            end = start + len(value_digits) - 1
            if end > 26:
                raise ValueError("fixed segment exceeds payload length 26")
            for offset, digit in enumerate(value_digits):
                position = start + offset
                existing = fixed_positions.get(position)
                if existing is not None and existing != digit:
                    raise ValueError(f"conflicting fixed digit for payload position {position}")
                fixed_positions[position] = digit

        for position, digit in fixed_positions.items():
            payload[position - 1] = digit

        mutable_positions = [index for index in range(26) if (index + 1) not in fixed_positions]
        if not mutable_positions:
            raise ValueError("no mutable payload digits remain after applying fixed segments")

        mutable_digits = "".join(payload[index] for index in mutable_positions)
        mutable_value = int(mutable_digits)
        next_mutable_value = mutable_value + step
        max_mutable_value = (10 ** len(mutable_positions)) - 1
        if next_mutable_value > max_mutable_value:
            raise ValueError("mutable payload overflow after applying fixed segments")

        next_mutable_digits = f"{next_mutable_value:0{len(mutable_positions)}d}"
        for index, digit in zip(mutable_positions, next_mutable_digits):
            payload[index] = digit

        return self.from_payload("".join(payload))


def generate_next_qrr(current_reference: str) -> dict[str, str]:
    """Convenience helper for direct use in invoice flows.

    Returns storage and visual strings for the next Swiss QR reference.
    """

    generator = SwissQRReferenceGenerator()
    next_ref = generator.next_from_reference(current_reference)
    return {
        "storage_string": next_ref.storage,
        "visual_string": next_ref.visual,
    }


if __name__ == "__main__":
    generator = SwissQRReferenceGenerator()

    # Example 1: sequence-based flow
    state = 26301
    result = generator.next_from_sequence(state)
    print("Sequence-based next reference")
    print(f"next_sequence_to_store: {result['next_sequence_to_store']}")
    print(f"storage_string: {result['storage_string']}")
    print(f"visual_string: {result['visual_string']}")

    # Example 2: from an existing 27-digit reference
    seed = "00 00000 00000 00000 00026 30181"
    if generator.validate(seed):
        nxt = generator.next_from_reference(seed)
        print("\nNext after seed reference")
        print(f"storage_string: {nxt.storage}")
        print(f"visual_string: {nxt.visual}")

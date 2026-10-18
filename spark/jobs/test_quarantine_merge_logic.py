BUSINESS_KEYS = ("order_id", "payment_sequential")


def merge_simulation(current, incoming):
    """Simulate an upsert by business key; no external storage."""
    for record in incoming:
        key = tuple(record[name] for name in BUSINESS_KEYS)
        current[key] = dict(record)
    return current


def validate_unique_keys(records):
    keys = [
        tuple(record[name] for name in BUSINESS_KEYS)
        for record in records
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate business keys in incoming records")


def main():
    records = [
        {
            "order_id": "744bade1fcf9ff3f31d860ace076d422",
            "payment_sequential": 2,
            "payment_installments": 0,
        },
        {
            "order_id": "1a57108394169c0b47d8f876acc9ba2d",
            "payment_sequential": 2,
            "payment_installments": 0,
        },
    ]

    validate_unique_keys(records)

    current = {}
    history = []

    # First attempt
    merge_simulation(current, records)
    history.append({"execution_id": "attempt-001"})
    assert len(current) == 2

    # Retry same keys: current records must not multiply.
    merge_simulation(current, records)
    assert len(current) == 2

    # A separate attempt is tracked independently.
    history.append({"execution_id": "attempt-002"})
    assert len(history) == 2
    assert len(current) == 2

    # Duplicate incoming keys must fail validation.
    duplicate_input = records + [dict(records[0])]
    try:
        validate_unique_keys(duplicate_input)
    except ValueError:
        pass
    else:
        raise AssertionError("Duplicate keys were not rejected")

    print("First attempt: 2 current quarantine records")
    print("Retry: 2 current quarantine records; no duplicates")
    print("Execution history simulation: 2 distinct attempts")
    print("Duplicate-source-key validation: PASS")
    print("Overall simulation: PASS")
    print("External storage accessed: False")
    print("Iceberg table created: False")
    print("S3 writes performed: False")


if __name__ == "__main__":
    main()


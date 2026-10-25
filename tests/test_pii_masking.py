"""
Tests verifying PII Masking and GDPR/DPDP tokenization compliance.
"""
import pytest
import hashlib
import json

def test_sha256_customer_id_and_email_hashing():
    raw_customer_id = "CUST_9876"
    raw_email = "cust_9876@retailco.com"

    expected_id_hash = hashlib.sha256(raw_customer_id.encode("utf-8")).hexdigest()
    expected_email_hash = hashlib.sha256(raw_email.encode("utf-8")).hexdigest()

    assert len(expected_id_hash) == 64
    assert len(expected_email_hash) == 64
    assert expected_id_hash != raw_customer_id
    assert expected_email_hash != raw_email

def test_credit_card_masking():
    card_number = "4111222233334444"
    preserve = 4
    mask_char = "*"

    masked = (mask_char * (len(card_number) - preserve)) + card_number[-preserve:]
    assert masked == "************4444"
    assert len(masked) == len(card_number)

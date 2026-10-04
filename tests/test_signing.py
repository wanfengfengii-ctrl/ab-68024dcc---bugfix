import unittest

from relay.signing import (
    body_fingerprint,
    canonical_body,
    sign,
    signature_valid,
    validate_alert_payload,
)


class SigningTests(unittest.TestCase):
    def test_canonical_body_is_order_independent(self):
        a = {"alertKey": "k", "station": "s", "sequence": 1,
             "level": "red", "observedAt": "t", "reading": 6.8}
        b = {"reading": 6.8, "observedAt": "t", "level": "red",
             "sequence": 1, "station": "s", "alertKey": "k"}
        self.assertEqual(canonical_body(a), canonical_body(b))
        self.assertEqual(body_fingerprint(a), body_fingerprint(b))

    def test_content_change_changes_fingerprint(self):
        a = {"alertKey": "k", "station": "s", "sequence": 1,
             "level": "red", "observedAt": "t", "reading": 6.8}
        b = dict(a, reading=6.9)
        self.assertNotEqual(body_fingerprint(a), body_fingerprint(b))

    def test_signature_roundtrip_and_tamper_detection(self):
        body = canonical_body({"alertKey": "k", "station": "s", "sequence": 1,
                               "level": "red", "observedAt": "t", "reading": 1})
        sig = sign("secret", "dlv-1", body)
        self.assertTrue(sig.startswith("sha256="))
        self.assertTrue(signature_valid("secret", "dlv-1", body, sig))
        self.assertFalse(signature_valid("wrong-secret", "dlv-1", body, sig))
        self.assertFalse(signature_valid("secret", "dlv-2", body, sig))
        self.assertFalse(signature_valid("secret", "dlv-1", body + b" ", sig))

    def test_validate_payload(self):
        good = {"alertKey": "k", "station": "s", "sequence": 0,
                "level": "red", "observedAt": "t", "reading": -1.5}
        self.assertEqual(validate_alert_payload(good), [])
        self.assertTrue(validate_alert_payload({"alertKey": "k"}))
        self.assertTrue(validate_alert_payload(dict(good, sequence=-1)))
        self.assertTrue(validate_alert_payload(dict(good, sequence=True)))
        self.assertTrue(validate_alert_payload(dict(good, reading="x")))
        self.assertTrue(validate_alert_payload(dict(good, alertKey="  ")))
        self.assertTrue(validate_alert_payload(dict(good, extra=1)))


if __name__ == "__main__":
    unittest.main()

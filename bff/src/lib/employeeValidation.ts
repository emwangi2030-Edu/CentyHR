/**
 * Format validators and normalizers for employee identity fields.
 * Used in the create/PATCH endpoints to enforce data quality before
 * writing to ERPNext.
 */

/** Kenyan KRA PIN: one uppercase letter, 9 digits, one uppercase letter. */
const KRA_PIN_RE = /^[A-Z]\d{9}[A-Z]$/;

/** Kenya National ID: 7–9 consecutive digits. */
const NATIONAL_ID_RE = /^\d{7,9}$/;

/** Loose international phone: optional leading +, then 7–15 digits (spaces/dashes allowed between). */
const PHONE_LOOSE_RE = /^\+?\d[\d\s\-]{5,17}\d$/;

/** Basic email sanity check. */
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/**
 * NSSF/NHIF/NITA: alphanumeric, 4–20 chars.
 * These vary widely; we only reject clearly invalid entries.
 */
const STATUTORY_NUM_RE = /^[A-Z0-9\-\/]{4,20}$/i;

// ── Validators — return error string or null ────────────────────────────────

export function validateKraPin(raw: string): string | null {
  const v = raw.trim().toUpperCase();
  if (!v) return null;
  if (!KRA_PIN_RE.test(v)) {
    return "KRA PIN must be one letter, 9 digits, one letter — e.g. A001234567Z";
  }
  return null;
}

export function validateNationalId(raw: string): string | null {
  const v = raw.trim();
  if (!v) return null;
  if (!NATIONAL_ID_RE.test(v)) {
    return "National ID must be 7–9 digits with no spaces or letters";
  }
  return null;
}

export function validatePhone(raw: string): string | null {
  const v = raw.trim();
  if (!v) return null;
  const stripped = v.replace(/[\s\-().]/g, "");
  if (!PHONE_LOOSE_RE.test(stripped)) {
    return "Phone number must be 7–15 digits — e.g. +254712345678 or 0712345678";
  }
  return null;
}

export function validateEmail(raw: string): string | null {
  const v = raw.trim();
  if (!v) return null;
  if (!EMAIL_RE.test(v)) {
    return "Enter a valid email address — e.g. jane@company.com";
  }
  return null;
}

export function validateStatutoryNumber(raw: string, label: string): string | null {
  const v = raw.trim();
  if (!v) return null;
  if (!STATUTORY_NUM_RE.test(v)) {
    return `${label} format looks invalid — expected 4–20 alphanumeric characters`;
  }
  return null;
}

// ── Normalizers — clean up values before storage ────────────────────────────

/** Uppercase and trim KRA PIN. */
export function normalizeKraPin(raw: string): string {
  return raw.trim().toUpperCase();
}

/**
 * Normalize Kenyan phone numbers to E.164 (+254XXXXXXXXX).
 * Non-Kenyan numbers (start with different country code) are returned stripped of spaces/dashes.
 */
export function normalizePhone(raw: string): string {
  const v = raw.trim();
  if (!v) return v;
  const stripped = v.replace(/[\s\-().]/g, "");
  // 07XXXXXXXX or 01XXXXXXXX → +254XXXXXXXXX
  if (/^0[17]\d{8}$/.test(stripped)) return `+254${stripped.slice(1)}`;
  // 254XXXXXXXXX → +254XXXXXXXXX
  if (/^254[17]\d{8}$/.test(stripped)) return `+${stripped}`;
  return stripped;
}

/** Trim and lowercase email. */
export function normalizeEmail(raw: string): string {
  return raw.trim().toLowerCase();
}

// ── Batch validation for a full employee doc ─────────────────────────────────

export type FieldError = { field: string; message: string };

/**
 * Run all format validations on a partial employee record.
 * Returns an array of field errors (empty = all valid).
 */
export function validateEmployeeDoc(doc: Record<string, unknown>): FieldError[] {
  const errors: FieldError[] = [];

  const phone = String(doc.cell_number ?? "");
  if (phone) {
    const e = validatePhone(phone);
    if (e) errors.push({ field: "cell_number", message: e });
  }

  const email = (field: string) => {
    const v = String(doc[field] ?? "");
    if (v) {
      const e = validateEmail(v);
      if (e) errors.push({ field, message: e });
    }
  };
  email("prefered_email");
  email("company_email");
  email("personal_email");

  const kra = String(doc.tax_id ?? "");
  if (kra) {
    const e = validateKraPin(kra);
    if (e) errors.push({ field: "tax_id", message: e });
  }

  const nid = String(doc.id_number ?? doc.custom_national_id ?? "");
  if (nid) {
    const e = validateNationalId(nid);
    if (e) errors.push({ field: "id_number", message: e });
  }

  return errors;
}
